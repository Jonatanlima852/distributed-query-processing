package storage

import (
	"path/filepath"
	"testing"

	"github.com/Jonatan852/distributed-query-processing/pkg/columnar"
)

func TestEngineIngestAndScan(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewEngine(filepath.Join(dir, "store"))
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	schema := TableSchema{
		Name: "events",
		Columns: []ColumnSchema{
			{Name: "user_id", Type: columnar.TypeInt},
			{Name: "value", Type: columnar.TypeFloat},
			{Name: "country", Type: columnar.TypeString},
		},
	}
	if err := engine.RegisterTable(schema); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	rows := []Row{
		{
			"user_id": columnar.NewIntValue(42),
			"value":   columnar.NewFloatValue(10.5),
			"country": columnar.NewStringValue("BR"),
		},
		{
			"user_id": columnar.NewIntValue(7),
			"value":   columnar.NewFloatValue(8.1),
			"country": columnar.NewStringValue("US"),
		},
		{
			"user_id": columnar.NewIntValue(42),
			"value":   columnar.NewFloatValue(13.4),
			"country": columnar.NewStringValue("BR"),
		},
	}

	if _, err := engine.Ingest("events", "part-01", rows); err != nil {
		t.Fatalf("ingest failed: %v", err)
	}

	var filtered int
	batches, err := engine.Scan("events", ScanOptions{
		Columns: []string{"user_id", "value"},
		Filter: func(row RowView) (bool, error) {
			val, err := row.Value("user_id")
			if err != nil {
				return false, err
			}
			id, _ := val.AsInt()
			return id == 42, nil
		},
		BatchSize: 1,
	})
	if err != nil {
		t.Fatalf("scan failed: %v", err)
	}
	if len(batches) != 2 {
		t.Fatalf("expected 2 batches (batchSize=1), got %d", len(batches))
	}
	for _, batch := range batches {
		filtered += batch.RowCount
	}
	if filtered != 2 {
		t.Fatalf("expected 2 rows after filter, got %d", filtered)
	}
}
