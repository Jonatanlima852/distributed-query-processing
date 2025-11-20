package storage

import "errors"

var (
	// ErrTableExists indicates that a table with the same name already exists in the catalog.
	ErrTableExists = errors.New("storage: table already exists")
	// ErrTableNotFound indicates that the referenced table is not registered.
	ErrTableNotFound = errors.New("storage: table not found")
	// ErrPartitionExists indicates that a partition with the same ID already exists.
	ErrPartitionExists = errors.New("storage: partition already exists")
	// ErrPartitionNotFound indicates that the referenced partition metadata is missing.
	ErrPartitionNotFound = errors.New("storage: partition not found")
)
