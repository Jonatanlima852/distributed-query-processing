package executor

import (
	"github.com/Jonatan852/distributed-query-processing/internal/storage"
	"github.com/Jonatan852/distributed-query-processing/pkg/columnar"
)

// StorageScanner abstrai o engine para facilitar testes.
type StorageScanner interface {
	Scan(table string, opts storage.ScanOptions) ([]storage.RecordBatch, error)
}

// ScanExecutor lê batches colunares do storage.
type ScanExecutor struct {
	engine  StorageScanner
	table   string
	options storage.ScanOptions

	batches []storage.RecordBatch
	index   int
	loaded  bool
}

func NewScanExecutor(engine StorageScanner, table string, opts storage.ScanOptions) *ScanExecutor {
	return &ScanExecutor{
		engine:  engine,
		table:   table,
		options: opts,
	}
}

func (s *ScanExecutor) load() error {
	if s.loaded {
		return nil
	}
	batches, err := s.engine.Scan(s.table, s.options)
	if err != nil {
		return err
	}
	s.batches = batches
	s.loaded = true
	return nil
}

func (s *ScanExecutor) Next() (*Batch, error) {
	if err := s.load(); err != nil {
		return nil, err
	}
	if s.index >= len(s.batches) {
		return nil, ErrNoMoreBatches
	}
	record := s.batches[s.index]
	s.index++
	columns := make(map[string]*columnar.Column, len(record.Columns))
	for name, col := range record.Columns {
		columns[name] = cloneColumn(col)
	}
	return &Batch{
		Columns:  columns,
		RowCount: record.RowCount,
		Meta: map[string]string{
			"table":     record.Table,
			"partition": record.Partition,
		},
	}, nil
}

func (s *ScanExecutor) Close() error {
	s.batches = nil
	return nil
}
