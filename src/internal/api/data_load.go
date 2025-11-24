package api

import (
	"fmt"
	"strings"
	"time"

	"github.com/Jonatan852/distributed-query-processing/internal/storage"
	"github.com/Jonatan852/distributed-query-processing/pkg/columnar"
)

type loadRequest struct {
	Table       string                   `json:"table"`
	Schema      *tableSchemaPayload      `json:"schema"`
	Rows        []map[string]interface{} `json:"rows"`
	PartitionID string                   `json:"partition_id"`
}

type tableSchemaPayload struct {
	Columns []columnSchemaPayload `json:"columns"`
}

type columnSchemaPayload struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

func (s *Server) applyLoadRequest(req loadRequest) error {
	if strings.TrimSpace(req.Table) == "" {
		return fmt.Errorf("campo table é obrigatório")
	}
	if len(req.Rows) == 0 {
		return fmt.Errorf("rows não pode ser vazio")
	}
	schema, err := s.ensureTableSchema(req.Table, req.Schema)
	if err != nil {
		return err
	}
	rows, err := buildRows(schema, req.Rows)
	if err != nil {
		return err
	}
	partitionID := req.PartitionID
	if partitionID == "" {
		partitionID = fmt.Sprintf("part-%d", time.Now().UnixNano())
	}
	_, err = s.cfg.Engine.Ingest(req.Table, partitionID, rows)
	return err
}

func (s *Server) ensureTableSchema(table string, payload *tableSchemaPayload) (storage.TableSchema, error) {
	schema, err := s.cfg.Engine.Table(table)
	if err == nil {
		return schema, nil
	}
	if err != storage.ErrTableNotFound {
		return storage.TableSchema{}, err
	}
	if payload == nil {
		return storage.TableSchema{}, fmt.Errorf("schema deve ser informado para tabelas novas")
	}
	if len(payload.Columns) == 0 {
		return storage.TableSchema{}, fmt.Errorf("columns não pode ser vazio")
	}
	columns := make([]storage.ColumnSchema, 0, len(payload.Columns))
	for _, col := range payload.Columns {
		dt, err := parseColumnType(col.Type)
		if err != nil {
			return storage.TableSchema{}, err
		}
		columns = append(columns, storage.ColumnSchema{
			Name: strings.ToLower(col.Name),
			Type: dt,
		})
	}
	schema = storage.TableSchema{
		Name:    table,
		Columns: columns,
	}
	if err := s.cfg.Engine.RegisterTable(schema); err != nil {
		return storage.TableSchema{}, err
	}
	return schema, nil
}

func buildRows(schema storage.TableSchema, data []map[string]interface{}) ([]storage.Row, error) {
	rows := make([]storage.Row, 0, len(data))
	for _, item := range data {
		row := storage.Row{}
		for _, col := range schema.Columns {
			raw, ok := item[col.Name]
			if !ok {
				return nil, fmt.Errorf("coluna %s ausente", col.Name)
			}
			value, err := convertValue(col.Type, raw)
			if err != nil {
				return nil, fmt.Errorf("coluna %s: %w", col.Name, err)
			}
			row[col.Name] = value
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func parseColumnType(name string) (columnar.DataType, error) {
	switch strings.ToUpper(name) {
	case "INT", "INT64":
		return columnar.TypeInt, nil
	case "STRING":
		return columnar.TypeString, nil
	case "FLOAT", "FLOAT64":
		return columnar.TypeFloat, nil
	case "BOOL", "BOOLEAN":
		return columnar.TypeBool, nil
	default:
		return 0, fmt.Errorf("tipo %s não suportado", name)
	}
}

func convertValue(dt columnar.DataType, raw interface{}) (columnar.Value, error) {
	switch dt {
	case columnar.TypeInt:
		switch v := raw.(type) {
		case float64:
			return columnar.NewIntValue(int64(v)), nil
		case int:
			return columnar.NewIntValue(int64(v)), nil
		case int64:
			return columnar.NewIntValue(v), nil
		default:
			return columnar.Value{}, fmt.Errorf("valor %v não é inteiro", raw)
		}
	case columnar.TypeFloat:
		switch v := raw.(type) {
		case float64:
			return columnar.NewFloatValue(v), nil
		case int:
			return columnar.NewFloatValue(float64(v)), nil
		case int64:
			return columnar.NewFloatValue(float64(v)), nil
		default:
			return columnar.Value{}, fmt.Errorf("valor %v não é float", raw)
		}
	case columnar.TypeString:
		if v, ok := raw.(string); ok {
			return columnar.NewStringValue(v), nil
		}
		return columnar.Value{}, fmt.Errorf("valor %v não é string", raw)
	case columnar.TypeBool:
		if v, ok := raw.(bool); ok {
			return columnar.NewBoolValue(v), nil
		}
		return columnar.Value{}, fmt.Errorf("valor %v não é boolean", raw)
	default:
		return columnar.Value{}, fmt.Errorf("tipo %s não suportado", dt)
	}
}
