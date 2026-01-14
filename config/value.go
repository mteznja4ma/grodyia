package config

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"time"
)

// atomicValue is a thread-safe value wrapper
type atomicValue struct {
	val any
}

// newAtomicValue creates a new atomic value
func newAtomicValue(val any) Value {
	return &atomicValue{val: val}
}

// Bool returns the value as bool
func (v *atomicValue) Bool() (bool, error) {
	switch val := v.val.(type) {
	case bool:
		return val, nil
	case string:
		return strconv.ParseBool(val)
	case int, int64, float64:
		return reflect.ValueOf(val).Float() != 0, nil
	default:
		return false, fmt.Errorf("cannot convert %T to bool", v.val)
	}
}

// Int returns the value as int64
func (v *atomicValue) Int() (int64, error) {
	switch val := v.val.(type) {
	case int:
		return int64(val), nil
	case int64:
		return val, nil
	case float64:
		return int64(val), nil
	case string:
		return strconv.ParseInt(val, 10, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to int", v.val)
	}
}

// Float returns the value as float64
func (v *atomicValue) Float() (float64, error) {
	switch val := v.val.(type) {
	case float64:
		return val, nil
	case float32:
		return float64(val), nil
	case int:
		return float64(val), nil
	case int64:
		return float64(val), nil
	case string:
		return strconv.ParseFloat(val, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to float", v.val)
	}
}

// String returns the value as string
func (v *atomicValue) String() (string, error) {
	switch val := v.val.(type) {
	case string:
		return val, nil
	case []byte:
		return string(val), nil
	default:
		return fmt.Sprintf("%v", v.val), nil
	}
}

// Duration returns the value as duration (nanoseconds)
func (v *atomicValue) Duration() (int64, error) {
	val, err := v.String()
	if err != nil {
		return 0, err
	}
	d, err := time.ParseDuration(val)
	if err != nil {
		return 0, err
	}
	return int64(d), nil
}

// Slice returns the value as slice of Values
func (v *atomicValue) Slice() ([]Value, error) {
	arr, ok := v.val.([]any)
	if !ok {
		return nil, fmt.Errorf("cannot convert %T to slice", v.val)
	}
	result := make([]Value, len(arr))
	for i, item := range arr {
		result[i] = newAtomicValue(item)
	}
	return result, nil
}

// Map returns the value as map of Values
func (v *atomicValue) Map() (map[string]Value, error) {
	m, ok := v.val.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("cannot convert %T to map", v.val)
	}
	result := make(map[string]Value, len(m))
	for k, val := range m {
		result[k] = newAtomicValue(val)
	}
	return result, nil
}

// Scan scans the value into a struct
func (v *atomicValue) Scan(dst any) error {
	data, err := json.Marshal(v.val)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, dst)
}

// Load returns the raw value
func (v *atomicValue) Load() any {
	return v.val
}
