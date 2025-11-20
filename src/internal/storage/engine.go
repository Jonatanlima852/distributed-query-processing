package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Jonatan852/distributed-query-processing/pkg/columnar"
)

// Engine manages catalog metadata and serialized partitions on disk.
type Engine struct {
	rootDir     string
	catalogPath string

	mu      sync.RWMutex
	catalog *Catalog
}

// NewEngine creates (or loads) a storage engine rooted at the provided directory.
func NewEngine(rootDir string) (*Engine, error) {
	if err := os.MkdirAll(rootDir, 0o755); err != nil {
		return nil, err
	}
	catalogPath := filepath.Join(rootDir, "catalog.json")
	catalog, err := LoadCatalog(catalogPath)
	if err != nil {
		return nil, err
	}
	return &Engine{
		rootDir:     rootDir,
		catalogPath: catalogPath,
		catalog:     catalog,
	}, nil
}

// RegisterTable adds a schema definition into the catalog.
func (e *Engine) RegisterTable(schema TableSchema) error {
	if err := schema.Validate(); err != nil {
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.catalog.Tables[schema.Name]; exists {
		return ErrTableExists
	}

	now := time.Now().UTC()
	e.catalog.Tables[schema.Name] = &TableMetadata{
		Name:       schema.Name,
		Schema:     schema,
		Partitions: map[string]*PartitionMetadata{},
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	return e.catalog.Save(e.catalogPath)
}

// Table returns a copy of the metadata for the requested table.
func (e *Engine) Table(name string) (TableSchema, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	meta, ok := e.catalog.Tables[name]
	if !ok {
		return TableSchema{}, ErrTableNotFound
	}
	return meta.Schema, nil
}

// ListTables returns schema information for all registered tables.
func (e *Engine) ListTables() []TableSchema {
	e.mu.RLock()
	defer e.mu.RUnlock()
	result := make([]TableSchema, 0, len(e.catalog.Tables))
	for _, table := range e.catalog.Tables {
		result = append(result, table.Schema)
	}
	return result
}

// Ingest stores rows as a new partition for the given table.
func (e *Engine) Ingest(tableName, partitionID string, rows []Row) (*PartitionMetadata, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	tableMeta, ok := e.catalog.Tables[tableName]
	if !ok {
		return nil, ErrTableNotFound
	}
	if _, exists := tableMeta.Partitions[partitionID]; exists {
		return nil, ErrPartitionExists
	}

	columns := make(map[string]*columnar.Column, len(tableMeta.Schema.Columns))
	for _, colSchema := range tableMeta.Schema.Columns {
		columns[colSchema.Name] = columnar.NewColumn(colSchema.Name, colSchema.Type)
	}

	for i, row := range rows {
		if err := tableMeta.Schema.ValidateRow(row); err != nil {
			return nil, fmt.Errorf("row %d: %w", i, err)
		}
		for _, colSchema := range tableMeta.Schema.Columns {
			if err := columns[colSchema.Name].Append(row[colSchema.Name]); err != nil {
				return nil, fmt.Errorf("row %d column %s: %w", i, colSchema.Name, err)
			}
		}
	}

	relativePath := filepath.Join(tableName, fmt.Sprintf("%s.gob", partitionID))
	fullPath := filepath.Join(e.rootDir, relativePath)
	if err := writePartition(fullPath, columns); err != nil {
		return nil, err
	}

	stats := computeStats(columns)
	rowCount := 0
	if len(tableMeta.Schema.Columns) > 0 {
		firstCol := columns[tableMeta.Schema.Columns[0].Name]
		rowCount = firstCol.Len()
	}
	now := time.Now().UTC()
	partitionMeta := &PartitionMetadata{
		ID:        partitionID,
		FilePath:  relativePath,
		RowCount:  rowCount,
		Stats:     stats,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if tableMeta.Partitions == nil {
		tableMeta.Partitions = map[string]*PartitionMetadata{}
	}
	tableMeta.Partitions[partitionID] = partitionMeta
	tableMeta.UpdatedAt = now

	if err := e.catalog.Save(e.catalogPath); err != nil {
		return nil, err
	}
	return partitionMeta, nil
}

// ensureTable fetches metadata or returns error used by scanning.
func (e *Engine) ensureTable(tableName string) (*TableMetadata, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	meta, ok := e.catalog.Tables[tableName]
	if !ok {
		return nil, ErrTableNotFound
	}
	return meta, nil
}
