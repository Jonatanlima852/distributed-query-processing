package executor

import "github.com/Jonatan852/distributed-query-processing/pkg/columnar"

// FilterExecutor aplica um predicado sobre batches do filho.
type FilterExecutor struct {
	child     Executor
	predicate Predicate
}

func NewFilterExecutor(child Executor, predicate Predicate) *FilterExecutor {
	return &FilterExecutor{child: child, predicate: predicate}
}

func (f *FilterExecutor) Next() (*Batch, error) {
	for {
		batch, err := f.child.Next()
		if err != nil {
			return nil, err
		}
		filtered := f.apply(batch)
		if filtered.RowCount == 0 {
			continue
		}
		return filtered, nil
	}
}

func (f *FilterExecutor) apply(batch *Batch) *Batch {
	result := &Batch{
		Columns:  make(map[string]*columnar.Column, len(batch.Columns)),
		RowCount: 0,
		Meta:     batch.Meta,
	}
	for name, col := range batch.Columns {
		result.Columns[name] = columnar.NewColumn(col.Name, col.Type)
	}
	row := batchRow{batch: batch}
	for i := 0; i < batch.RowCount; i++ {
		row.index = i
		pass := true
		if f.predicate != nil {
			ok, err := f.predicate(row)
			if err != nil {
				pass = false
			} else {
				pass = ok
			}
		}
		if !pass {
			continue
		}
		for name, col := range batch.Columns {
			value, _ := col.Get(i)
			_ = addColumnData(result.Columns[name], value)
		}
		result.RowCount++
	}
	return result
}

func (f *FilterExecutor) Close() error {
	return f.child.Close()
}
