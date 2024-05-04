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
