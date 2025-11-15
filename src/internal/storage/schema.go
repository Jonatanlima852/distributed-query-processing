package storage

import (
	"fmt"
	"strings"

	"github.com/Jonatan852/distributed-query-processing/pkg/columnar"
)

// ColumnSchema describes a single column stored in the engine.
type ColumnSchema struct {
	Name     string            `json:"name"`
	Type     columnar.DataType `json:"type"`
	Encoding string            `json:"encoding,omitempty"`
	Comment  string            `json:"comment,omitempty"`
	Stats    *ColumnStats      `json:"stats,omitempty"`
	Tags     map[string]string `json:"tags,omitempty"`
}

// TableSchema contains metadata about a table.
type TableSchema struct {
	Name         string            `json:"name"`
	Columns      []ColumnSchema    `json:"columns"`
	PrimaryKeys  []string          `json:"primaryKeys,omitempty"`
	PartitionKey string            `json:"partitionKey,omitempty"`
	Properties   map[string]string `json:"properties,omitempty"`
}

// Validate ensures the schema definition is sane.
func (ts TableSchema) Validate() error {
	if strings.TrimSpace(ts.Name) == "" {
		return fmt.Errorf("table name is required")
	}
	if len(ts.Columns) == 0 {
		return fmt.Errorf("table %s must have at least one column", ts.Name)
	}
	seen := map[string]struct{}{}
	for _, col := range ts.Columns {
		if strings.TrimSpace(col.Name) == "" {
			return fmt.Errorf("table %s has a column without name", ts.Name)
		}
		lower := strings.ToLower(col.Name)
		if _, ok := seen[lower]; ok {
			return fmt.Errorf("duplicated column %s in table %s", col.Name, ts.Name)
		}
		seen[lower] = struct{}{}
	}
	return nil
}

// ColumnNames returns the ordered list of column names defined in the schema.
func (ts TableSchema) ColumnNames() []string {
	names := make([]string, 0, len(ts.Columns))
	for _, col := range ts.Columns {
		names = append(names, col.Name)
	}
	return names
}

// ColumnByName returns the schema entry for the requested column, ignoring case.
func (ts TableSchema) ColumnByName(name string) (ColumnSchema, bool) {
	for _, col := range ts.Columns {
		if strings.EqualFold(col.Name, name) {
			return col, true
		}
	}
	return ColumnSchema{}, false
}

// Row represents a single tuple ready to be ingested in the storage engine.
type Row map[string]columnar.Value

// ValidateRow ensures that the provided row matches the schema definition.
func (ts TableSchema) ValidateRow(row Row) error {
	for _, col := range ts.Columns {
		val, ok := row[col.Name]
		if !ok {
			return fmt.Errorf("missing value for column %q", col.Name)
		}
		if val.Type != col.Type {
			return fmt.Errorf("column %s expects %s but received %s", col.Name, col.Type, val.Type)
		}
	}
	return nil
}
