package storage

import (
	"encoding/gob"
	"os"
	"path/filepath"

	"github.com/Jonatan852/distributed-query-processing/pkg/columnar"
)

type partitionPayload struct {
	Columns map[string]*columnar.Column
}

func init() {
	gob.Register(&columnar.Column{})
}

func writePartition(path string, columns map[string]*columnar.Column) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	payload := partitionPayload{Columns: columns}
	encoder := gob.NewEncoder(file)
	return encoder.Encode(&payload)
}

func readPartition(path string) (map[string]*columnar.Column, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var payload partitionPayload
	if err := gob.NewDecoder(file).Decode(&payload); err != nil {
		return nil, err
	}
	return payload.Columns, nil
}
