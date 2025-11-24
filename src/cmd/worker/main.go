package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/Jonatan852/distributed-query-processing/internal/distributed"
	"github.com/Jonatan852/distributed-query-processing/internal/storage"
	"github.com/Jonatan852/distributed-query-processing/pkg/query"
)

type registrationResponse struct {
	ID            string `json:"id"`
	Secret        string `json:"secret"`
	PollPath      string `json:"poll"`
	ResultPath    string `json:"result"`
	HeartbeatPath string `json:"heartbeat"`
}

type pollResponse struct {
	Task *distributed.TaskRequest `json:"task"`
}

func main() {
	var (
		id       = flag.String("id", "", "ID do worker (opcional, será gerado se vazio)")
		dataDir  = flag.String("data-dir", "./data", "Diretório com partições locais")
		coordURL = flag.String("coordinator", "http://localhost:8080", "URL do coordinator")
		idleWait = flag.Duration("idle-wait", 3*time.Second, "Tempo de espera quando não há tasks")
	)
	flag.Parse()

	engine, err := storage.NewEngine(*dataDir)
	if err != nil {
		log.Fatalf("erro abrindo storage: %v", err)
	}

	reg, err := registerWorker(*coordURL, *id)
	if err != nil {
		log.Fatalf("falha ao registrar worker: %v", err)
	}
	log.Printf("worker %s registrado no coordinator", reg.ID)

	client := &http.Client{Timeout: 30 * time.Second}
	for {
		task, err := pollTask(client, *coordURL, reg)
		if err != nil {
			log.Printf("poll falhou: %v", err)
			time.Sleep(*idleWait)
			continue
		}
		if task == nil {
			time.Sleep(*idleWait)
			continue
		}
		result := executeFragment(engine, task.Fragment)
		result.TaskID = task.TaskID
		if err := sendResult(client, *coordURL, reg, result); err != nil {
			log.Printf("erro enviando resultado: %v", err)
		}
	}
}

func registerWorker(coordURL, id string) (registrationResponse, error) {
	payload := map[string]string{"id": id}
	data, _ := json.Marshal(payload)
	resp, err := http.Post(joinURL(coordURL, "/workers/register"), "application/json", bytes.NewReader(data))
	if err != nil {
		return registrationResponse{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return registrationResponse{}, fmt.Errorf("registro retornou %s", resp.Status)
	}
	var reg registrationResponse
	if err := json.NewDecoder(resp.Body).Decode(&reg); err != nil {
		return registrationResponse{}, err
	}
	return reg, nil
}

func pollTask(client *http.Client, coordURL string, reg registrationResponse) (*distributed.TaskRequest, error) {
	req, err := http.NewRequest(http.MethodPost, joinURL(coordURL, reg.PollPath), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Worker-Secret", reg.Secret)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNoContent {
		return nil, nil
	}
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("poll retornou %s", resp.Status)
	}
	var pr pollResponse
	if err := json.NewDecoder(resp.Body).Decode(&pr); err != nil {
		return nil, err
	}
	return pr.Task, nil
}

func sendResult(client *http.Client, coordURL string, reg registrationResponse, result distributed.TaskResult) error {
	data, _ := json.Marshal(result)
	req, err := http.NewRequest(http.MethodPost, joinURL(coordURL, reg.ResultPath), bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("X-Worker-Secret", reg.Secret)
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("envio retornou %s", resp.Status)
	}
	return nil
}

func joinURL(base, endpoint string) string {
	if strings.HasSuffix(base, "/") {
		base = strings.TrimRight(base, "/")
	}
	return base + path.Clean("/"+endpoint)
}

func executeFragment(engine *storage.Engine, node *query.PlanNode) distributed.TaskResult {
	start := time.Now()
	if node == nil {
		return distributed.TaskResult{Error: "fragmento vazio"}
	}
	if node.Type != query.PlanNodeScan {
		return distributed.TaskResult{Error: fmt.Sprintf("nó %s não suportado no worker", node.Type)}
	}
	table, _ := node.Properties["table"].(string)
	if table == "" {
		return distributed.TaskResult{Error: "fragmento sem tabela"}
	}
	batches, err := engine.Scan(table, storage.ScanOptions{})
	if err != nil {
		return distributed.TaskResult{Error: err.Error()}
	}
	total := 0
	for _, batch := range batches {
		total += batch.RowCount
	}
	return distributed.TaskResult{
		Rows:     total,
		Duration: time.Since(start),
	}
}
