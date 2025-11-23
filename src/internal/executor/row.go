package executor

import (
	"fmt"

	"github.com/Jonatan852/distributed-query-processing/pkg/columnar"
)

type batchRow struct {
	batch *Batch
	index int
}

func (r batchRow) Value(column string) (columnar.Value, error) {
	col, ok := r.batch.Columns[column]
	if !ok {
		return columnar.Value{}, fmt.Errorf("coluna %s não encontrada", column)
	}
	return col.Get(r.index)
}
