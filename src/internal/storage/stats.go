package storage

import "github.com/Jonatan852/distributed-query-processing/pkg/columnar"

func computeStats(columns map[string]*columnar.Column) map[string]ColumnStats {
	stats := make(map[string]ColumnStats, len(columns))
	for name, col := range columns {
		stats[name] = summarizeColumn(col)
	}
	return stats
}

func summarizeColumn(col *columnar.Column) ColumnStats {
	if col == nil {
		return ColumnStats{}
	}

	switch col.Type {
	case columnar.TypeInt:
		return summarizeInt(col)
	case columnar.TypeFloat:
		return summarizeFloat(col)
	case columnar.TypeString:
		return summarizeString(col)
	case columnar.TypeBool:
		return summarizeBool(col)
	default:
		return ColumnStats{}
	}
}

func summarizeInt(col *columnar.Column) ColumnStats {
	if len(col.IntData) == 0 {
		return ColumnStats{}
	}
	min := col.IntData[0]
	max := col.IntData[0]
	for _, v := range col.IntData[1:] {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}
	return ColumnStats{
		Count: len(col.IntData),
		Min:   FromValue(columnar.NewIntValue(min)),
		Max:   FromValue(columnar.NewIntValue(max)),
	}
}

func summarizeFloat(col *columnar.Column) ColumnStats {
	if len(col.FloatData) == 0 {
		return ColumnStats{}
	}
	min := col.FloatData[0]
	max := col.FloatData[0]
	for _, v := range col.FloatData[1:] {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}
	return ColumnStats{
		Count: len(col.FloatData),
		Min:   FromValue(columnar.NewFloatValue(min)),
		Max:   FromValue(columnar.NewFloatValue(max)),
	}
}

func summarizeString(col *columnar.Column) ColumnStats {
	if len(col.StringData) == 0 {
		return ColumnStats{}
	}
	min := col.StringData[0]
	max := col.StringData[0]
	for _, v := range col.StringData[1:] {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}
	return ColumnStats{
		Count: len(col.StringData),
		Min:   FromValue(columnar.NewStringValue(min)),
		Max:   FromValue(columnar.NewStringValue(max)),
	}
}

func summarizeBool(col *columnar.Column) ColumnStats {
	if len(col.BoolData) == 0 {
		return ColumnStats{}
	}
	min := col.BoolData[0]
	max := col.BoolData[0]
	for _, v := range col.BoolData[1:] {
		if !v {
			min = false
		}
		if v {
			max = true
		}
	}
	return ColumnStats{
		Count: len(col.BoolData),
		Min:   FromValue(columnar.NewBoolValue(min)),
		Max:   FromValue(columnar.NewBoolValue(max)),
	}
}
