package executor

import (
	"errors"
	"fmt"

	"github.com/Jonatan852/distributed-query-processing/pkg/columnar"
)

// ErrNoMoreBatches indica o fim do fluxo de dados.
var ErrNoMoreBatches = errors.New("executor: não há mais batches")

// Batch representa o bloco de linhas trocado entre executores.
type Batch struct {
	Columns  map[string]*columnar.Column
	RowCount int
	Meta     map[string]string
}

// Executor define a interface comum de operadores.
type Executor interface {
	Next() (*Batch, error)
	Close() error
}

// RowView permite ler valores durante filtros/joins.
type RowView interface {
	Value(column string) (columnar.Value, error)
}

// Predicate avalia se uma linha deve ser mantida.
type Predicate func(RowView) (bool, error)

func cloneColumn(col *columnar.Column) *columnar.Column {
	if col == nil {
		return nil
	}
	return col.Clone()
}

func addColumnData(col *columnar.Column, value columnar.Value) error {
	if col == nil {
		return fmt.Errorf("coluna destino inválida")
	}
	return col.Append(value)
}
