package conversions

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// StructToMap converts a struct to a map[string]interface{} based on the provided tag name.
// It handles nested structs and slices of structs recursively.
// Returns an error if the input is not a struct or pointer to a struct.
func StructToMap(data interface{}, tagName string) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	v := reflect.ValueOf(data)

	// Dereference pointer if necessary
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return nil, errors.New("nil pointer passed to StructToMap")
		}
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		return nil, errors.New("StructToMap expects a struct or a pointer to a struct")
	}

	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		field := t.Field(i)

		// Skip unexported fields
		if field.PkgPath != "" {
			continue
		}

		tagValue := field.Tag.Get(tagName)
		var key string
		if tagValue != "" {
			key = strings.Split(tagValue, ",")[0] // Use the first part before any comma
		} else {
			key = field.Name
		}

		fieldValue := v.Field(i).Interface()

		// Handle nested structs, slices, and maps recursively
		convertedValue, err := convertValue(fieldValue, tagName)
		if err != nil {
			return nil, fmt.Errorf("error converting field '%s': %v", field.Name, err)
		}

		result[key] = convertedValue
	}

	return result, nil
}

// convertValue recursively converts a value to map[string]interface{}, handling structs, slices, and maps.
// Parameters:
// - value: The value to convert.
// - tagName: The struct tag name used for mapping.
//
// Returns:
// - The converted value.
// - An error if the conversion fails.
func convertValue(value interface{}, tagName string) (interface{}, error) {
	val := reflect.ValueOf(value)

	switch val.Kind() {
	case reflect.Ptr:
		if val.IsNil() {
			return nil, nil
		}
		return convertValue(val.Elem().Interface(), tagName)
	case reflect.Struct:
		return StructToMap(val.Interface(), tagName)
	case reflect.Slice, reflect.Array:
		var slice []interface{}
		for i := 0; i < val.Len(); i++ {
			elem := val.Index(i).Interface()
			convertedElem, err := convertValue(elem, tagName)
			if err != nil {
				return nil, err
			}
			slice = append(slice, convertedElem)
		}
		return slice, nil
	case reflect.Map:
		// Only handle map[string]interface{}
		if val.Type().Key().Kind() != reflect.String {
			return nil, errors.New("only map with string keys are supported")
		}
		mapped := make(map[string]interface{})
		for _, key := range val.MapKeys() {
			mapVal := val.MapIndex(key).Interface()
			convertedVal, err := convertValue(mapVal, tagName)
			if err != nil {
				return nil, err
			}
			mapped[key.String()] = convertedVal
		}
		return mapped, nil
	default:
		return value, nil
	}
}

func TruncateTimestampToHour(unixNano int64) int64 {
	const nanosecondsPerHour = 3600 * 1_000_000_000 // 3.6 trillion nanoseconds in an hour
	return (unixNano / nanosecondsPerHour) * nanosecondsPerHour
}
