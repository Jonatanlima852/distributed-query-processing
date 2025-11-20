package storage

import (
	"fmt"
	"path/filepath"
	"sort"

	"github.com/Jonatan852/distributed-query-processing/pkg/columnar"
)

// RecordBatch is the unit returned by scans, containing a subset of rows.
type RecordBatch struct {
	Table     string
	Partition string
	Columns   map[string]*columnar.Column
	RowCount  int
}

// FilterFunc can be used to trim rows during scans.
type FilterFunc func(RowView) (bool, error)

// ScanOptions control projection, predicate and partition selection.
type ScanOptions struct {
	Columns    []string
	Partitions []string
	Filter     FilterFunc
	BatchSize  int
}

// RowView provides read-only access to row values for filter predicates.
type RowView interface {
	Value(column string) (columnar.Value, error)
}

// Scan iterates over selected partitions returning batches that satisfy the predicate.
func (e *Engine) Scan(tableName string, opts ScanOptions) ([]RecordBatch, error) {
	tableMeta, err := e.ensureTable(tableName)
	if err != nil {
		return nil, err
	}
	projected := opts.Columns
	if len(projected) == 0 {
		projected = tableMeta.Schema.ColumnNames()
	}
	if err := validateProjection(tableMeta.Schema, projected); err != nil {
		return nil, err
	}
	partitions := opts.Partitions
	if len(partitions) == 0 {
		for _, meta := range tableMeta.SortedPartitions() {
			partitions = append(partitions, meta.ID)
		}
	} else {
		sort.Strings(partitions)
	}
	if len(partitions) == 0 {
		return []RecordBatch{}, nil
	}
	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = 4096
	}

	result := make([]RecordBatch, 0)
	for _, partitionID := range partitions {
		partitionMeta, ok := tableMeta.Partitions[partitionID]
		if !ok {
			return nil, ErrPartitionNotFound
		}
		fullPath := filepath.Join(e.rootDir, partitionMeta.FilePath)
		columns, err := readPartition(fullPath)
		if err != nil {
			return nil, fmt.Errorf("partition %s: %w", partitionID, err)
		}
		rowLen := partitionMeta.RowCount
		if rowLen == 0 && len(columns) > 0 {
			if firstCol, ok := columns[projected[0]]; ok && firstCol != nil {
				rowLen = firstCol.Len()
			}
		}
		filteredIdx := make([]int, 0, batchSize)
		row := rowAccessor{columns: columns}
		for rowIndex := 0; rowIndex < rowLen; rowIndex++ {
			if opts.Filter != nil {
				row.index = rowIndex
				pass, err := opts.Filter(row)
				if err != nil {
					return nil, err
				}
				if !pass {
					continue
				}
			}
			filteredIdx = append(filteredIdx, rowIndex)
			if len(filteredIdx) == batchSize {
				result = append(result, buildBatch(tableName, partitionID, columns, projected, filteredIdx))
				filteredIdx = filteredIdx[:0]
			}
		}
		if len(filteredIdx) > 0 {
			result = append(result, buildBatch(tableName, partitionID, columns, projected, filteredIdx))
		}
	}
	return result, nil
}

func validateProjection(schema TableSchema, projected []string) error {
	for _, name := range projected {
		if _, ok := schema.ColumnByName(name); !ok {
			return fmt.Errorf("unknown column %s in projection", name)
		}
	}
	return nil
}

func buildBatch(table, partition string, columns map[string]*columnar.Column, projected []string, indexes []int) RecordBatch {
	result := RecordBatch{
		Table:     table,
		Partition: partition,
		Columns:   make(map[string]*columnar.Column, len(projected)),
		RowCount:  len(indexes),
	}
	for _, name := range projected {
		result.Columns[name] = projectColumn(columns[name], indexes)
	}
	return result
}

func projectColumn(col *columnar.Column, indexes []int) *columnar.Column {
	if col == nil {
		return nil
	}
	out := columnar.NewColumn(col.Name, col.Type)
	switch col.Type {
	case columnar.TypeInt:
		out.IntData = make([]int64, 0, len(indexes))
		for _, idx := range indexes {
			out.IntData = append(out.IntData, col.IntData[idx])
		}
	case columnar.TypeFloat:
		out.FloatData = make([]float64, 0, len(indexes))
		for _, idx := range indexes {
			out.FloatData = append(out.FloatData, col.FloatData[idx])
		}
	case columnar.TypeString:
		out.StringData = make([]string, 0, len(indexes))
		for _, idx := range indexes {
			out.StringData = append(out.StringData, col.StringData[idx])
		}
	case columnar.TypeBool:
		out.BoolData = make([]bool, 0, len(indexes))
		for _, idx := range indexes {
			out.BoolData = append(out.BoolData, col.BoolData[idx])
		}
	}
	return out
}

type rowAccessor struct {
	columns map[string]*columnar.Column
	index   int
}

func (r rowAccessor) Value(column string) (columnar.Value, error) {
	col, ok := r.columns[column]
	if !ok {
		return columnar.Value{}, fmt.Errorf("column %s not found in row", column)
	}
	return col.Get(r.index)
}
