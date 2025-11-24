package executor

import (
	"testing"

	"github.com/Jonatan852/distributed-query-processing/internal/storage"
	"github.com/Jonatan852/distributed-query-processing/pkg/columnar"
)

type fakeScanner struct {
	batches []storage.RecordBatch
}

func (f fakeScanner) Scan(table string, opts storage.ScanOptions) ([]storage.RecordBatch, error) {
	return f.batches, nil
}

func TestScanFilterAggregate(t *testing.T) {
	userCol := columnar.NewColumn("user_id", columnar.TypeInt)
	countryCol := columnar.NewColumn("country", columnar.TypeString)
	valueCol := columnar.NewColumn("amount", columnar.TypeFloat)
	for i := 0; i < 5; i++ {
		_ = userCol.Append(columnar.NewIntValue(int64(i % 2)))
		if i%2 == 0 {
			_ = countryCol.Append(columnar.NewStringValue("BR"))
		} else {
			_ = countryCol.Append(columnar.NewStringValue("US"))
		}
		_ = valueCol.Append(columnar.NewFloatValue(float64(i) + 1))
	}
	fake := fakeScanner{
		batches: []storage.RecordBatch{
			{
				Table:     "events",
				Partition: "p1",
				Columns: map[string]*columnar.Column{
					"user_id": userCol,
					"country": countryCol,
					"amount":  valueCol,
				},
				RowCount: userCol.Len(),
			},
		},
	}

	scan := NewScanExecutor(fake, "events", storage.ScanOptions{})
	filter := NewFilterExecutor(scan, func(row RowView) (bool, error) {
		val, _ := row.Value("country")
		str, _ := val.AsString()
		return str == "BR", nil
	})
	agg := NewAggregateExecutor(filter, []string{"country"}, []AggregateSpec{
		{Func: AggregateCount, Column: "*", Alias: "total"},
		{Func: AggregateSum, Column: "amount", Alias: "sum_amount"},
	})

	result, err := agg.Next()
	if err != nil {
		t.Fatalf("aggregate falhou: %v", err)
	}
	if result.RowCount != 1 {
		t.Fatalf("esperava 1 grupo, obteve %d", result.RowCount)
	}
	countCol := result.Columns["total"]
	val, _ := countCol.Get(0)
	total, _ := val.AsInt()
	if total != 3 {
		t.Fatalf("esperava 3 linhas filtradas, obteve %d", total)
	}
}
