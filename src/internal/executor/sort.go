package executor

import (
	"sort"

	"github.com/Jonatan852/distributed-query-processing/pkg/columnar"
)

// SortKey define cada coluna usada na ordenação.
type SortKey struct {
	Column    string
	Ascending bool
}

// SortExecutor agrega todas as linhas em memória e ordena antes de devolver batches.
type SortExecutor struct {
	child     Executor
	keys      []SortKey
	buffer    *Batch
	emitted   bool
	batchSize int
}

func NewSortExecutor(child Executor, keys []SortKey, batchSize int) *SortExecutor {
	if batchSize <= 0 {
		batchSize = 1024
	}
	return &SortExecutor{
		child:     child,
		keys:      keys,
		batchSize: batchSize,
	}
}

func (s *SortExecutor) Next() (*Batch, error) {
	if s.buffer == nil {
		if err := s.loadAndSort(); err != nil {
			return nil, err
		}
	}
	if s.buffer.RowCount == 0 {
		return nil, ErrNoMoreBatches
	}
	rowsToEmit := min(s.batchSize, s.buffer.RowCount)
	result := &Batch{
		Columns:  map[string]*columnar.Column{},
		RowCount: rowsToEmit,
	}
	for name, col := range s.buffer.Columns {
		slice, _ := col.Slice(0, rowsToEmit)
		result.Columns[name] = slice
	}
	// Remove linhas emitidas
	for name, col := range s.buffer.Columns {
		remaining, _ := col.Slice(rowsToEmit, col.Len())
		s.buffer.Columns[name] = remaining
	}
	s.buffer.RowCount -= rowsToEmit
	return result, nil
}

func (s *SortExecutor) loadAndSort() error {
	rows := []map[string]columnar.Value{}
	for {
		batch, err := s.child.Next()
		if err != nil {
			if err == ErrNoMoreBatches {
				break
			}
			return err
		}
		row := batchRow{batch: batch}
		for i := 0; i < batch.RowCount; i++ {
			row.index = i
			record := map[string]columnar.Value{}
			for name := range batch.Columns {
				val, _ := row.Value(name)
				record[name] = val
			}
			rows = append(rows, record)
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		for _, key := range s.keys {
			left := rows[i][key.Column]
			right := rows[j][key.Column]
			comp := compare(left, right)
			if comp == 0 {
				continue
			}
			if key.Ascending {
				return comp < 0
			}
			return comp > 0
		}
		return false
	})

	if len(rows) == 0 {
		s.buffer = &Batch{Columns: map[string]*columnar.Column{}, RowCount: 0}
		return nil
	}
	columns := map[string]*columnar.Column{}
	for name, val := range rows[0] {
		columns[name] = columnar.NewColumn(name, val.Type)
	}
	for _, row := range rows {
		for name, val := range row {
			_ = addColumnData(columns[name], val)
		}
	}
	s.buffer = &Batch{
		Columns:  columns,
		RowCount: len(rows),
	}
	return nil
}

func (s *SortExecutor) Close() error {
	return s.child.Close()
}

func compare(left, right columnar.Value) int {
	switch left.Type {
	case columnar.TypeInt:
		l, _ := left.AsInt()
		r, _ := right.AsInt()
		switch {
		case l < r:
			return -1
		case l > r:
			return 1
		default:
			return 0
		}
	case columnar.TypeFloat:
		l, _ := left.AsFloat()
		r, _ := right.AsFloat()
		switch {
		case l < r:
			return -1
		case l > r:
			return 1
		default:
			return 0
		}
	case columnar.TypeString:
		l, _ := left.AsString()
		r, _ := right.AsString()
		switch {
		case l < r:
			return -1
		case l > r:
			return 1
		default:
			return 0
		}
	default:
		return 0
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
