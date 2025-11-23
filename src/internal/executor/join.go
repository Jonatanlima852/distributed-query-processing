package executor

import (
	"fmt"

	"github.com/Jonatan852/distributed-query-processing/pkg/columnar"
)

// JoinType representa o tipo de join suportado.
type JoinType string

const (
	JoinTypeInner JoinType = "INNER"
)

// JoinCondition define colunas de comparação (colunas devem estar presentes nos batches).
type JoinCondition struct {
	LeftColumn  string
	RightColumn string
}

// HashJoinExecutor realiza um hash join simples em memória.
type HashJoinExecutor struct {
	left       Executor
	right      Executor
	condition  JoinCondition
	hashTable  map[string][]map[string]columnar.Value
	leftSchema map[string]columnar.DataType
}

func NewHashJoinExecutor(left, right Executor, cond JoinCondition) *HashJoinExecutor {
	return &HashJoinExecutor{
		left:       left,
		right:      right,
		condition:  cond,
		hashTable:  map[string][]map[string]columnar.Value{},
		leftSchema: map[string]columnar.DataType{},
	}
}

func (j *HashJoinExecutor) Next() (*Batch, error) {
	if j.hashTable == nil {
		if err := j.buildHashTable(); err != nil {
			return nil, err
		}
	}
	for {
		rightBatch, err := j.right.Next()
		if err != nil {
			return nil, err
		}
		result := j.probe(rightBatch)
		if result.RowCount > 0 {
			return result, nil
		}
	}
}

func (j *HashJoinExecutor) buildHashTable() error {
	j.hashTable = map[string][]map[string]columnar.Value{}
	for {
		leftBatch, err := j.left.Next()
		if err != nil {
			if err == ErrNoMoreBatches {
				break
			}
			return err
		}
		row := batchRow{batch: leftBatch}
		for i := 0; i < leftBatch.RowCount; i++ {
			row.index = i
			key, err := row.Value(j.condition.LeftColumn)
			if err != nil {
				return err
			}
			record := map[string]columnar.Value{}
			for name := range leftBatch.Columns {
				value, _ := row.Value(name)
				record[name] = value
				j.leftSchema[name] = value.Type
			}
			j.hashTable[key.String()] = append(j.hashTable[key.String()], record)
		}
	}
	return nil
}

func (j *HashJoinExecutor) probe(batch *Batch) *Batch {
	result := &Batch{
		Columns:  map[string]*columnar.Column{},
		RowCount: 0,
	}
	for name, typ := range j.leftSchema {
		result.Columns[name] = columnar.NewColumn(name, typ)
	}
	for name, col := range batch.Columns {
		outputName := fmt.Sprintf("right.%s", name)
		result.Columns[outputName] = columnar.NewColumn(outputName, col.Type)
	}
	row := batchRow{batch: batch}
	for i := 0; i < batch.RowCount; i++ {
		row.index = i
		key, err := row.Value(j.condition.RightColumn)
		if err != nil {
			continue
		}
		matches := j.hashTable[key.String()]
		for _, match := range matches {
			for name, val := range match {
				_ = addColumnData(result.Columns[name], val)
			}
			for name := range batch.Columns {
				val, _ := row.Value(name)
				_ = addColumnData(result.Columns["right."+name], val)
			}
			result.RowCount++
		}
	}
	return result
}

func (j *HashJoinExecutor) Close() error {
	if err := j.left.Close(); err != nil {
		return err
	}
	return j.right.Close()
}
