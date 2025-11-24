package runner

import (
	"path/filepath"
	"testing"

	"github.com/Jonatan852/distributed-query-processing/internal/parser"
	"github.com/Jonatan852/distributed-query-processing/internal/storage"
	"github.com/Jonatan852/distributed-query-processing/pkg/columnar"
)

func TestRunnerExecuteSimpleSelect(t *testing.T) {
	dir := t.TempDir()
	engine, err := storage.NewEngine(filepath.Join(dir, "data"))
	if err != nil {
		t.Fatalf("erro criando engine: %v", err)
	}
	schema := storage.TableSchema{
		Name: "events",
		Columns: []storage.ColumnSchema{
			{Name: "user_id", Type: columnar.TypeInt},
			{Name: "country", Type: columnar.TypeString},
		},
	}
	if err := engine.RegisterTable(schema); err != nil {
		t.Fatalf("erro registrando tabela: %v", err)
	}
	rows := []storage.Row{
		{"user_id": columnar.NewIntValue(1), "country": columnar.NewStringValue("BR")},
		{"user_id": columnar.NewIntValue(2), "country": columnar.NewStringValue("US")},
		{"user_id": columnar.NewIntValue(1), "country": columnar.NewStringValue("BR")},
	}
	if _, err := engine.Ingest("events", "p1", rows); err != nil {
		t.Fatalf("erro ao ingerir dados: %v", err)
	}

	sql := `SELECT user_id, country FROM events WHERE user_id = 1 LIMIT 1`
	stmt, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("parser falhou: %v", err)
	}

	r := New(engine)
	result, err := r.Execute(stmt)
	if err != nil {
		t.Fatalf("runner falhou: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("esperava 1 linha, obteve %d", len(result))
	}
	if got := result[0]["user_id"]; got != int64(1) {
		t.Fatalf("user_id incorreto: %v", got)
	}
	if got := result[0]["country"]; got != "BR" {
		t.Fatalf("country incorreto: %v", got)
	}
}
