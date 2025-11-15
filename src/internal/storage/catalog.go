package storage

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/Jonatan852/distributed-query-processing/pkg/columnar"
)

// Catalog represents the persisted description of tables and partitions.
type Catalog struct {
	Tables map[string]*TableMetadata `json:"tables"`
}

// TableMetadata keeps schema definition and partitions per table.
type TableMetadata struct {
	Name       string                        `json:"name"`
	Schema     TableSchema                   `json:"schema"`
	Partitions map[string]*PartitionMetadata `json:"partitions"`
	CreatedAt  time.Time                     `json:"createdAt"`
	UpdatedAt  time.Time                     `json:"updatedAt"`
}

// PartitionMetadata points to the serialized columnar files and holds statistics.
type PartitionMetadata struct {
	ID        string                 `json:"id"`
	FilePath  string                 `json:"filePath"`
	RowCount  int                    `json:"rowCount"`
	Stats     map[string]ColumnStats `json:"stats"`
	CreatedAt time.Time              `json:"createdAt"`
	UpdatedAt time.Time              `json:"updatedAt"`
	Tags      map[string]string      `json:"tags,omitempty"`
}

// ColumnStats stores basic min/max/NULL counts used for pruning.
type ColumnStats struct {
	Count     int          `json:"count"`
	NullCount int          `json:"nullCount"`
	Min       *ScalarValue `json:"min,omitempty"`
	Max       *ScalarValue `json:"max,omitempty"`
}

// ScalarValue is a JSON-friendly representation of columnar.Value.
type ScalarValue struct {
	Type        columnar.DataType `json:"type"`
	IntValue    *int64            `json:"int,omitempty"`
	StringValue *string           `json:"string,omitempty"`
	FloatValue  *float64          `json:"float,omitempty"`
	BoolValue   *bool             `json:"bool,omitempty"`
}

// FromValue converts a columnar.Value into a ScalarValue.
func FromValue(v columnar.Value) *ScalarValue {
	result := &ScalarValue{Type: v.Type}
	switch v.Type {
	case columnar.TypeInt:
		i, _ := v.AsInt()
		result.IntValue = &i
	case columnar.TypeString:
		s, _ := v.AsString()
		result.StringValue = &s
	case columnar.TypeFloat:
		f, _ := v.AsFloat()
		result.FloatValue = &f
	case columnar.TypeBool:
		b, _ := v.AsBool()
		result.BoolValue = &b
	default:
		return nil
	}
	return result
}

// ToValue converts a ScalarValue back to columnar.Value.
func (s *ScalarValue) ToValue() columnar.Value {
	if s == nil {
		return columnar.Value{}
	}
	switch s.Type {
	case columnar.TypeInt:
		if s.IntValue != nil {
			return columnar.NewIntValue(*s.IntValue)
		}
	case columnar.TypeString:
		if s.StringValue != nil {
			return columnar.NewStringValue(*s.StringValue)
		}
	case columnar.TypeFloat:
		if s.FloatValue != nil {
			return columnar.NewFloatValue(*s.FloatValue)
		}
	case columnar.TypeBool:
		if s.BoolValue != nil {
			return columnar.NewBoolValue(*s.BoolValue)
		}
	}
	return columnar.Value{}
}

// Save writes the catalog json to disk.
func (c *Catalog) Save(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// LoadCatalog reads catalog info or returns empty structure when missing.
func LoadCatalog(path string) (*Catalog, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &Catalog{Tables: map[string]*TableMetadata{}}, nil
		}
		return nil, err
	}
	var catalog Catalog
	if err := json.Unmarshal(data, &catalog); err != nil {
		return nil, err
	}
	if catalog.Tables == nil {
		catalog.Tables = map[string]*TableMetadata{}
	}
	return &catalog, nil
}

// SortedPartitions returns partition metadata sorted by ID.
func (tm *TableMetadata) SortedPartitions() []*PartitionMetadata {
	ids := make([]string, 0, len(tm.Partitions))
	for id := range tm.Partitions {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	partitions := make([]*PartitionMetadata, 0, len(ids))
	for _, id := range ids {
		partitions = append(partitions, tm.Partitions[id])
	}
	return partitions
}
