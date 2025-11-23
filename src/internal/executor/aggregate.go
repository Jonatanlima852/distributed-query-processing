package executor

import (
	"fmt"
	"strings"

	"github.com/Jonatan852/distributed-query-processing/pkg/columnar"
)

// AggregateFunc representa o tipo de agregação.
type AggregateFunc string

const (
	AggregateCount AggregateFunc = "COUNT"
	AggregateSum   AggregateFunc = "SUM"
	AggregateMin   AggregateFunc = "MIN"
	AggregateMax   AggregateFunc = "MAX"
	AggregateAvg   AggregateFunc = "AVG"
)

// AggregateSpec descreve cada medida calculada.
type AggregateSpec struct {
	Func   AggregateFunc
	Column string
	Alias  string
}

// AggregateExecutor processa todos os batches em memória e devolve um único resultado.
type AggregateExecutor struct {
	child      Executor
	groupKeys  []string
	aggregates []AggregateSpec
	result     *Batch
	emitted    bool
}

func NewAggregateExecutor(child Executor, groupKeys []string, specs []AggregateSpec) *AggregateExecutor {
	return &AggregateExecutor{
		child:      child,
		groupKeys:  groupKeys,
		aggregates: specs,
	}
}

func (a *AggregateExecutor) Next() (*Batch, error) {
	if a.emitted {
		return nil, ErrNoMoreBatches
	}
	if err := a.compute(); err != nil {
		return nil, err
	}
	a.emitted = true
	return a.result, nil
}

func (a *AggregateExecutor) compute() error {
	state := map[string]*aggState{}
	for {
		batch, err := a.child.Next()
		if err != nil {
			if err == ErrNoMoreBatches {
				break
			}
			return err
		}
		row := batchRow{batch: batch}
		for i := 0; i < batch.RowCount; i++ {
			row.index = i
			key := a.buildKey(row)
			if _, ok := state[key]; !ok {
				state[key] = newAggState(a.groupKeys, a.aggregates)
				state[key].setGroupValues(row)
			}
			if err := state[key].accumulate(row); err != nil {
				return err
			}
		}
	}

	if len(state) == 0 {
		a.result = &Batch{
			Columns:  map[string]*columnar.Column{},
			RowCount: 0,
		}
		return nil
	}

	columns := map[string]*columnar.Column{}
	for _, entry := range state {
		for name, value := range entry.groupValues {
			if _, ok := columns[name]; !ok {
				typ := value.Type
				if typ == 0 {
					typ = columnar.TypeString
				}
				columns[name] = columnar.NewColumn(name, typ)
			}
		}
		break
	}
	for _, spec := range a.aggregates {
		name := spec.Alias
		if name == "" {
			name = fmt.Sprintf("%s(%s)", spec.Func, spec.Column)
		}
		columns[name] = columnar.NewColumn(name, columnType(spec.Func))
	}

	for _, entry := range state {
		for name, value := range entry.groupValues {
			if _, ok := columns[name]; !ok {
				columns[name] = columnar.NewColumn(name, value.Type)
			}
			_ = addColumnData(columns[name], value)
		}
		for idx, spec := range a.aggregates {
			val := entry.aggregates[idx].finalize(spec.Func)
			name := spec.Alias
			if name == "" {
				name = fmt.Sprintf("%s(%s)", spec.Func, spec.Column)
			}
			_ = addColumnData(columns[name], val)
		}
	}

	a.result = &Batch{
		Columns:  columns,
		RowCount: len(state),
	}
	return nil
}

func (a *AggregateExecutor) buildKey(row batchRow) string {
	if len(a.groupKeys) == 0 {
		return "__all__"
	}
	values := make([]string, 0, len(a.groupKeys))
	for _, key := range a.groupKeys {
		val, err := row.Value(key)
		if err != nil {
			values = append(values, "")
			continue
		}
		values = append(values, val.String())
	}
	return strings.Join(values, "|")
}

func (a *AggregateExecutor) Close() error {
	return a.child.Close()
}

type aggState struct {
	groupKeys   []string
	groupValues map[string]columnar.Value
	aggregates  []aggAccumulator
	specs       []AggregateSpec
}

func newAggState(keys []string, specs []AggregateSpec) *aggState {
	accs := make([]aggAccumulator, len(specs))
	for i, spec := range specs {
		accs[i] = newAccumulator(spec.Func)
	}
	return &aggState{
		groupKeys:   keys,
		groupValues: map[string]columnar.Value{},
		aggregates:  accs,
		specs:       specs,
	}
}

func (s *aggState) setGroupValues(row batchRow) {
	for _, key := range s.groupKeys {
		val, err := row.Value(key)
		if err != nil {
			s.groupValues[key] = columnar.Value{}
			continue
		}
		s.groupValues[key] = val
	}
}

func (s *aggState) accumulate(row batchRow) error {
	for idx, spec := range s.specs {
		var val columnar.Value
		if spec.Func == AggregateCount && spec.Column == "*" {
			val = columnar.NewIntValue(1)
		} else {
			var err error
			val, err = row.Value(spec.Column)
			if err != nil {
				return err
			}
		}
		if err := s.aggregates[idx].accumulate(val); err != nil {
			return err
		}
	}
	return nil
}

type aggAccumulator interface {
	accumulate(value columnar.Value) error
	finalize(fn AggregateFunc) columnar.Value
}

type numericAccumulator struct {
	count int64
	sum   float64
	min   *float64
	max   *float64
}

func newAccumulator(fn AggregateFunc) aggAccumulator {
	return &numericAccumulator{}
}

func (a *numericAccumulator) accumulate(value columnar.Value) error {
	var v float64
	switch value.Type {
	case columnar.TypeInt:
		i, _ := value.AsInt()
		v = float64(i)
	case columnar.TypeFloat:
		f, _ := value.AsFloat()
		v = f
	default:
		return fmt.Errorf("agregador suporta apenas INT/FLOAT")
	}
	a.count++
	a.sum += v
	if a.min == nil || v < *a.min {
		a.min = &v
	}
	if a.max == nil || v > *a.max {
		a.max = &v
	}
	return nil
}

func (a *numericAccumulator) finalize(fn AggregateFunc) columnar.Value {
	switch fn {
	case AggregateCount:
		return columnar.NewIntValue(a.count)
	case AggregateSum:
		return columnar.NewFloatValue(a.sum)
	case AggregateAvg:
		if a.count == 0 {
			return columnar.NewFloatValue(0)
		}
		return columnar.NewFloatValue(a.sum / float64(a.count))
	case AggregateMin:
		if a.min == nil {
			return columnar.NewFloatValue(0)
		}
		return columnar.NewFloatValue(*a.min)
	case AggregateMax:
		if a.max == nil {
			return columnar.NewFloatValue(0)
		}
		return columnar.NewFloatValue(*a.max)
	default:
		return columnar.NewFloatValue(0)
	}
}

func columnType(fn AggregateFunc) columnar.DataType {
	switch fn {
	case AggregateCount:
		return columnar.TypeInt
	default:
		return columnar.TypeFloat
	}
}
