package columnar

import "fmt"

// DataType representa os tipos de dados suportados no sistema colunar
type DataType int

const (
	TypeInt DataType = iota
	TypeString
	TypeFloat
	TypeBool
)

// String retorna a representação em string do tipo
func (dt DataType) String() string {
	switch dt {
	case TypeInt:
		return "INT"
	case TypeString:
		return "STRING"
	case TypeFloat:
		return "FLOAT"
	case TypeBool:
		return "BOOL"
	default:
		return "UNKNOWN"
	}
}

// Value representa um valor genérico que pode ser de qualquer tipo suportado
type Value struct {
	Type DataType
	Data interface{}
}

// NewIntValue cria um novo valor inteiro
func NewIntValue(v int64) Value {
	return Value{Type: TypeInt, Data: v}
}

// NewStringValue cria um novo valor string
func NewStringValue(v string) Value {
	return Value{Type: TypeString, Data: v}
}

// NewFloatValue cria um novo valor float
func NewFloatValue(v float64) Value {
	return Value{Type: TypeFloat, Data: v}
}

// NewBoolValue cria um novo valor booleano
func NewBoolValue(v bool) Value {
	return Value{Type: TypeBool, Data: v}
}

// AsInt retorna o valor como int64, ou erro se o tipo for incompatível
func (v Value) AsInt() (int64, error) {
	if v.Type != TypeInt {
		return 0, fmt.Errorf("value is not an int, got %s", v.Type)
	}
	return v.Data.(int64), nil
}

// AsString retorna o valor como string, ou erro se o tipo for incompatível
func (v Value) AsString() (string, error) {
	if v.Type != TypeString {
		return "", fmt.Errorf("value is not a string, got %s", v.Type)
	}
	return v.Data.(string), nil
}

// AsFloat retorna o valor como float64, ou erro se o tipo for incompatível
func (v Value) AsFloat() (float64, error) {
	if v.Type != TypeFloat {
		return 0, fmt.Errorf("value is not a float, got %s", v.Type)
	}
	return v.Data.(float64), nil
}

// AsBool retorna o valor como bool, ou erro se o tipo for incompatível
func (v Value) AsBool() (bool, error) {
	if v.Type != TypeBool {
		return false, fmt.Errorf("value is not a bool, got %s", v.Type)
	}
	return v.Data.(bool), nil
}

// String retorna a representação em string do valor
func (v Value) String() string {
	return fmt.Sprintf("%v", v.Data)
}
