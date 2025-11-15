package columnar

import "fmt"

// Column representa uma coluna de dados homogêneos
// Armazena valores do mesmo tipo de forma contígua em memória
type Column struct {
	Name string
	Type DataType
	// Usamos diferentes slices para cada tipo para eficiência
	IntData    []int64
	StringData []string
	FloatData  []float64
	BoolData   []bool
	// Bitmap para valores NULL (opcional, futuro)
	// NullBitmap []bool
}

// NewColumn cria uma nova coluna com o nome e tipo especificados
func NewColumn(name string, dataType DataType) *Column {
	return &Column{
		Name: name,
		Type: dataType,
	}
}

// Append adiciona um valor à coluna
func (c *Column) Append(value Value) error {
	if value.Type != c.Type {
		return fmt.Errorf("type mismatch: column is %s, got %s", c.Type, value.Type)
	}

	switch c.Type {
	case TypeInt:
		c.IntData = append(c.IntData, value.Data.(int64))
	case TypeString:
		c.StringData = append(c.StringData, value.Data.(string))
	case TypeFloat:
		c.FloatData = append(c.FloatData, value.Data.(float64))
	case TypeBool:
		c.BoolData = append(c.BoolData, value.Data.(bool))
	default:
		return fmt.Errorf("unsupported type: %s", c.Type)
	}

	return nil
}

// Get retorna o valor na posição especificada
func (c *Column) Get(index int) (Value, error) {
	if index < 0 || index >= c.Len() {
		return Value{}, fmt.Errorf("index out of bounds: %d (len: %d)", index, c.Len())
	}

	switch c.Type {
	case TypeInt:
		return NewIntValue(c.IntData[index]), nil
	case TypeString:
		return NewStringValue(c.StringData[index]), nil
	case TypeFloat:
		return NewFloatValue(c.FloatData[index]), nil
	case TypeBool:
		return NewBoolValue(c.BoolData[index]), nil
	default:
		return Value{}, fmt.Errorf("unsupported type: %s", c.Type)
	}
}

// Len retorna o número de elementos na coluna
func (c *Column) Len() int {
	switch c.Type {
	case TypeInt:
		return len(c.IntData)
	case TypeString:
		return len(c.StringData)
	case TypeFloat:
		return len(c.FloatData)
	case TypeBool:
		return len(c.BoolData)
	default:
		return 0
	}
}

// Clone cria uma cópia da coluna
func (c *Column) Clone() *Column {
	newCol := &Column{
		Name: c.Name,
		Type: c.Type,
	}

	switch c.Type {
	case TypeInt:
		newCol.IntData = make([]int64, len(c.IntData))
		copy(newCol.IntData, c.IntData)
	case TypeString:
		newCol.StringData = make([]string, len(c.StringData))
		copy(newCol.StringData, c.StringData)
	case TypeFloat:
		newCol.FloatData = make([]float64, len(c.FloatData))
		copy(newCol.FloatData, c.FloatData)
	case TypeBool:
		newCol.BoolData = make([]bool, len(c.BoolData))
		copy(newCol.BoolData, c.BoolData)
	}

	return newCol
}

// Slice retorna uma nova coluna com um subset de dados [start:end)
func (c *Column) Slice(start, end int) (*Column, error) {
	if start < 0 || end > c.Len() || start > end {
		return nil, fmt.Errorf("invalid slice range: [%d:%d] for length %d", start, end, c.Len())
	}

	newCol := &Column{
		Name: c.Name,
		Type: c.Type,
	}

	switch c.Type {
	case TypeInt:
		newCol.IntData = c.IntData[start:end]
	case TypeString:
		newCol.StringData = c.StringData[start:end]
	case TypeFloat:
		newCol.FloatData = c.FloatData[start:end]
	case TypeBool:
		newCol.BoolData = c.BoolData[start:end]
	}

	return newCol, nil
}
