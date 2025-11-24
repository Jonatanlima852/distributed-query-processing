package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Jonatan852/distributed-query-processing/internal/api"
	"github.com/Jonatan852/distributed-query-processing/internal/distributed"
	"github.com/Jonatan852/distributed-query-processing/internal/planner"
	runtimerunner "github.com/Jonatan852/distributed-query-processing/internal/runtime/runner"
	"github.com/Jonatan852/distributed-query-processing/internal/storage"
	"github.com/Jonatan852/distributed-query-processing/pkg/query"
)

func main() {
	var (
		httpAddr        = flag.String("http-addr", ":8080", "Endereço HTTP para expor a API")
		dataDir         = flag.String("data-dir", "./data", "Diretório do storage local")
		embeddedWorkers = flag.Int("embedded-workers", 0, "Número de workers locais registrados automaticamente")
	)
	flag.Parse()

	engine, err := storage.NewEngine(*dataDir)
	if err != nil {
		log.Fatalf("falha ao abrir storage: %v", err)
	}
	coord := distributed.NewCoordinator()
	plan := planner.New(engine)
	queryRunner := runtimerunner.New(engine)

	for i := 0; i < *embeddedWorkers; i++ {
		id := fmt.Sprintf("embedded-%d", i+1)
		worker := distributed.NewLocalWorker(id, func(req distributed.TaskRequest) distributed.TaskResult {
			return executeFragment(engine, req.Fragment, id, req.TaskID)
		})
		coord.Register(worker)
	}

	server, err := api.NewServer(api.Config{
		Addr:         *httpAddr,
		Engine:       engine,
		Planner:      plan,
		Coordinator:  coord,
		Runner:       queryRunner,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 0, // long-poll em /workers/* não deve expirar cedo
	})
	if err != nil {
		log.Fatalf("erro criando API: %v", err)
	}

	log.Printf("coordinator escutando em %s (workers embarcados: %d)", *httpAddr, *embeddedWorkers)
	go func() {
		if err := server.Start(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("erro no servidor: %v", err)
		}
	}()

	// aguarda sinal para encerramento
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Printf("encerrando coordinator...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(ctx)
}

func executeFragment(engine *storage.Engine, node *query.PlanNode, workerID, taskID string) distributed.TaskResult {
	start := time.Now()
	if node == nil {
		return distributed.TaskResult{TaskID: taskID, WorkerID: workerID, Error: "fragmento vazio"}
	}
	if node.Type != query.PlanNodeScan {
		return distributed.TaskResult{TaskID: taskID, WorkerID: workerID, Error: fmt.Sprintf("nó %s não suportado", node.Type)}
	}
	table, _ := node.Properties["table"].(string)
	if table == "" {
		return distributed.TaskResult{TaskID: taskID, WorkerID: workerID, Error: "fragmento sem tabela"}
	}
	batches, err := engine.Scan(table, storage.ScanOptions{})
	if err != nil {
		return distributed.TaskResult{TaskID: taskID, WorkerID: workerID, Error: err.Error()}
	}
	rows := 0
	for _, batch := range batches {
		rows += batch.RowCount
	}
	return distributed.TaskResult{
		TaskID:   taskID,
		WorkerID: workerID,
		Rows:     rows,
		Duration: time.Since(start),
	}
}
