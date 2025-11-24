package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/Jonatan852/distributed-query-processing/internal/storage"
	"github.com/Jonatan852/distributed-query-processing/pkg/columnar"
)

func main() {
	var (
		dataDir   = flag.String("data-dir", "./data", "Diretório do storage")
		rows      = flag.Int("rows", 1000, "Quantidade de linhas sintéticas por partição")
		table     = flag.String("table", "events", "Nome da tabela")
		partition = flag.String("partition", "", "ID da partição (gerado automaticamente se vazio)")
	)
	flag.Parse()

	engine, err := storage.NewEngine(*dataDir)
	if err != nil {
		log.Fatalf("erro abrindo storage: %v", err)
	}

	schema := storage.TableSchema{
		Name: *table,
		Columns: []storage.ColumnSchema{
			{Name: "user_id", Type: columnar.TypeInt},
			{Name: "event_type", Type: columnar.TypeString},
			{Name: "ts", Type: columnar.TypeString},
			{Name: "value", Type: columnar.TypeFloat},
		},
	}
	if _, err := engine.Table(*table); err == storage.ErrTableNotFound {
		if err := engine.RegisterTable(schema); err != nil {
			log.Fatalf("erro registrando tabela: %v", err)
		}
	}

	rowsData := make([]storage.Row, 0, *rows)
	now := time.Now()
	for i := 0; i < *rows; i++ {
		row := storage.Row{
			"user_id":    columnar.NewIntValue(int64(rand.Intn(1000))),
			"event_type": columnar.NewStringValue(fmt.Sprintf("event_%d", rand.Intn(5))),
			"ts":         columnar.NewStringValue(now.Add(time.Duration(i) * time.Second).Format(time.RFC3339)),
			"value":      columnar.NewFloatValue(rand.Float64() * 100),
		}
		rowsData = append(rowsData, row)
	}
	partID := *partition
	if partID == "" {
		partID = fmt.Sprintf("part-%d", time.Now().UnixNano())
	}
	if _, err := engine.Ingest(*table, partID, rowsData); err != nil {
		log.Fatalf("falha ao ingerir dados: %v", err)
	}
	log.Printf("partição %s (%d linhas) gravada em %s", partID, len(rowsData), *dataDir)
}
