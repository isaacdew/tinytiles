package cmd

import (
	"encoding/json"
	"fmt"
)

type Layer struct {
	Id          string `json:"id"`
	Filter      Filter `json:"filter"`
	SourceLayer string `json:"source-layer"`
}

type Style struct {
	Layers []Layer `json:"layers"`
}

type Filter interface {
	isFilter()
}

type ComparisonFilter struct {
	Op     string
	Field  string
	Values []interface{}
}

func (ComparisonFilter) isFilter() {}

type LogicalFilter struct {
	Op      string
	Filters []Filter
}

func (LogicalFilter) isFilter() {}

func UnmarshalFilter(data []byte) (Filter, error) {
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		fmt.Println("Error")
		return nil, err
	}

	if len(raw) == 0 {
		return nil, fmt.Errorf("empty filter")
	}

	var op string
	if err := json.Unmarshal(raw[0], &op); err != nil {
		return nil, err
	}

	// TODO: Modify to use comparison vs logical based on value type and not depend on operator
	switch op {
	case "==", "!=", ">", "<", ">=", "<=", "in", "!in", "at", "!has", "index-of", "slice", "global-state", "get", "has", "length", "case", "match", "coalesce":
		var field string
		if err := json.Unmarshal(raw[1], &field); err != nil {
			return nil, err
		}
		var values []interface{}
		for _, v := range raw[2:] {
			var val interface{}
			if err := json.Unmarshal(v, &val); err != nil {
				return nil, err
			}
			values = append(values, val)
		}
		return ComparisonFilter{Op: op, Field: field, Values: values}, nil

	case "all", "any", "none":
		var filters []Filter
		for _, sub := range raw[1:] {
			f, err := UnmarshalFilter(sub)
			if err != nil {
				return nil, err
			}
			filters = append(filters, f)
		}
		return LogicalFilter{Op: op, Filters: filters}, nil

	default:
		fmt.Println(op)
		return nil, fmt.Errorf("unknown filter op: %s", op)
	}
}

func (r *Layer) UnmarshalJSON(data []byte) error {
	type alias Layer
	aux := &struct {
		Filter json.RawMessage `json:"filter"`
		*alias
	}{
		alias: (*alias)(r),
	}

	if err := json.Unmarshal(data, aux); err != nil {
		return err
	}

	if aux.Filter != nil {
		filter, err := UnmarshalFilter(aux.Filter)
		if err != nil {
			return err
		}
		r.Filter = filter
	}

	return nil
}

func collectFields(f Filter, fields map[string]struct{}) {
	switch f := f.(type) {
	case ComparisonFilter:
		if f.Field != "" {
			fields[f.Field] = struct{}{}
		}
	case LogicalFilter:
		for _, sub := range f.Filters {
			collectFields(sub, fields)
		}
	}
}

func getFields(f Filter) []string {
	seen := make(map[string]struct{})
	collectFields(f, seen)

	result := make([]string, 0, len(seen))
	for k := range seen {
		if _, ok := seen[k]; ok {
			result = append(result, k)
		}
	}
	return result
}
