package utils

import (
	"math/rand"
	"time"

	. "github.com/farbodahm/streame/pkg/types"
)

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

// HeavyRecordSchema returns schema for the HeavyRecord
func HeavyRecordSchema() Schema {
	return Schema{
		Columns: Fields{
			"field_1":  StringType,
			"field_2":  StringType,
			"field_3":  StringType,
			"field_4":  StringType,
			"field_5":  StringType,
			"field_6":  StringType,
			"field_7":  StringType,
			"field_8":  StringType,
			"field_9":  StringType,
			"field_10": StringType,
			"field_11": StringType,
			"field_12": StringType,
			"field_13": StringType,
			"field_14": StringType,
			"field_15": StringType,
			"field_16": StringType,
			"field_17": StringType,
			"field_18": StringType,
			"field_19": StringType,
			"field_20": StringType,
		},
	}
}

// NewHeavyRecord creates a new heavy Record with random string values
// with given length.
// You can use this for benchmark tests.
func NewHeavyRecord(string_length int) Record {
	return Record{
		Key: GenerateRandomString(string_length),
		Data: ValueMap{
			"field_1":  String{Val: GenerateRandomString(string_length)},
			"field_2":  String{Val: GenerateRandomString(string_length)},
			"field_3":  String{Val: GenerateRandomString(string_length)},
			"field_4":  String{Val: GenerateRandomString(string_length)},
			"field_5":  String{Val: GenerateRandomString(string_length)},
			"field_6":  String{Val: GenerateRandomString(string_length)},
			"field_7":  String{Val: GenerateRandomString(string_length)},
			"field_8":  String{Val: GenerateRandomString(string_length)},
			"field_9":  String{Val: GenerateRandomString(string_length)},
			"field_10": String{Val: GenerateRandomString(string_length)},
			"field_11": String{Val: GenerateRandomString(string_length)},
			"field_12": String{Val: GenerateRandomString(string_length)},
			"field_13": String{Val: GenerateRandomString(string_length)},
			"field_14": String{Val: GenerateRandomString(string_length)},
			"field_15": String{Val: GenerateRandomString(string_length)},
			"field_16": String{Val: GenerateRandomString(string_length)},
			"field_17": String{Val: GenerateRandomString(string_length)},
			"field_18": String{Val: GenerateRandomString(string_length)},
			"field_19": String{Val: GenerateRandomString(string_length)},
			"field_20": String{Val: GenerateRandomString(string_length)},
		},
	}
}
