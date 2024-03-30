package utils

import (
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"time"
)

// HeavyStruct is a heavy object to be used for benchmarking tests.
type HeavyStruct struct {
	Field1  string
	Field2  string
	Field3  string
	Field4  string
	Field5  string
	Field6  string
	Field7  string
	Field8  string
	Field9  string
	Field10 string
	Field11 string
	Field12 string
	Field13 string
	Field14 string
	Field15 string
	Field16 string
	Field17 string
	Field18 string
	Field19 string
	Field20 string
}

// GenerateRandomString generates a random string of the specified length.
func GenerateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	var seededRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))

	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

// NewHeavyStruct creates a new HeavyStruct with random string values
// with given length.
func NewHeavyStruct(string_length int) HeavyStruct {
	return HeavyStruct{
		Field1:  GenerateRandomString(string_length),
		Field2:  GenerateRandomString(string_length),
		Field3:  GenerateRandomString(string_length),
		Field4:  GenerateRandomString(string_length),
		Field5:  GenerateRandomString(string_length),
		Field6:  GenerateRandomString(string_length),
		Field7:  GenerateRandomString(string_length),
		Field8:  GenerateRandomString(string_length),
		Field9:  GenerateRandomString(string_length),
		Field10: GenerateRandomString(string_length),
		Field11: GenerateRandomString(string_length),
		Field12: GenerateRandomString(string_length),
		Field13: GenerateRandomString(string_length),
		Field14: GenerateRandomString(string_length),
		Field15: GenerateRandomString(string_length),
		Field16: GenerateRandomString(string_length),
		Field17: GenerateRandomString(string_length),
		Field18: GenerateRandomString(string_length),
		Field19: GenerateRandomString(string_length),
		Field20: GenerateRandomString(string_length),
	}
}

// TODO: After adding schema support to SDF, we shouldn't need this function.
// ConvertStructToMap converts a struct to a map[string]string.
// It uses reflection to iterate over the struct's fields.
// Note: This function does not handle nested structs or pointer fields.
func ConvertStructToMap(s interface{}) (map[string]string, error) {
	v := reflect.ValueOf(s)
	if v.Kind() != reflect.Struct {
		return nil, fmt.Errorf("expected struct, got %s", v.Kind())
	}

	t := v.Type()
	result := make(map[string]string)

	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		fieldName := t.Field(i).Name

		// Convert each field to string in a basic way.
		// You might want to customize this part based on your needs.
		var valueStr string
		switch field.Kind() {
		case reflect.String:
			valueStr = field.String()
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			valueStr = strconv.FormatInt(field.Int(), 10)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			valueStr = strconv.FormatUint(field.Uint(), 10)
		case reflect.Float32, reflect.Float64:
			valueStr = strconv.FormatFloat(field.Float(), 'f', -1, 64)
		case reflect.Bool:
			valueStr = strconv.FormatBool(field.Bool())
		default:
			return nil, fmt.Errorf("field %s has unsupported type %s", fieldName, field.Kind())
		}

		result[fieldName] = valueStr
	}

	return result, nil
}
