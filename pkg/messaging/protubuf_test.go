package messaging_test

import (
	"fmt"
	"log"
	"reflect"
	"testing"
	"time"

	"github.com/farbodahm/streame/pkg/messaging"
	. "github.com/farbodahm/streame/pkg/types"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Ensure TestType implements ColumnValue
var _ ColumnValue = TestType{}

// TestType implements ColumnValue for test scenarios
type TestType struct {
	Val int
}

func (t TestType) Value() any {
	return -1
}

func (t TestType) ToInt() int {
	return -1
}

func (t TestType) ToString() string {
	return "test_string"
}

func (t TestType) ToArray() []ColumnValue {
	panic("TestType cannot be converted to an array")
}

func (t TestType) Type() ColumnType {
	return 9999
}

func TestValueMapToProtoStruct_ValidRecord_RecordShouldConvertToProtobufStruct(t *testing.T) {
	record := Record{
		Key: "key1",
		Data: ValueMap{
			"first_name": String{Val: "foobar"},
			"last_name":  String{Val: "random_lastname"},
			"age":        Integer{Val: 23},
		},
	}

	result, err := messaging.ValueMapToProtoStruct(record.Data)
	expectedStruct := structpb.Struct{
		Fields: map[string]*structpb.Value{
			"first_name": structpb.NewStringValue("foobar"),
			"last_name":  structpb.NewStringValue("random_lastname"),
			"age":        structpb.NewNumberValue(23),
		},
	}

	assert.Nil(t, err)
	assert.Equal(t, expectedStruct.AsMap(), result.AsMap())
}

func TestValueMapToProtoStruct_RecordWithInvalidType_PanicsWithError(t *testing.T) {
	record := Record{
		Key: "key1",
		Data: ValueMap{
			"first_name": String{Val: "foobar"},
			"age":        TestType{Val: 23},
		},
	}

	expectedError := fmt.Sprintf(messaging.ErrConvertingToProtoStruct, "9999")

	assert.PanicsWithError(t, expectedError, func() {
		_, _ = messaging.ValueMapToProtoStruct(record.Data)
	}, "Expected panic with error: %s", expectedError)
}

func TestValueMapToProtocolBuffers_ValidRecord_RecordMarshalsToProtobuf(t *testing.T) {
	record := Record{
		Key: "key1",
		Data: ValueMap{
			"first_name": String{Val: "foobar"},
			"last_name":  String{Val: "random_lastname"},
			"age":        Integer{Val: 23},
		},
	}
	expectedStruct := structpb.Struct{
		Fields: map[string]*structpb.Value{
			"first_name": structpb.NewStringValue("foobar"),
			"last_name":  structpb.NewStringValue("random_lastname"),
			"age":        structpb.NewNumberValue(23),
		},
	}

	result, err := messaging.ValueMapToProtocolBuffers(record.Data)
	assert.Nil(t, err)

	var resultDeserialized messaging.RecordData
	err = proto.Unmarshal(result, &resultDeserialized)
	assert.Nil(t, err)
	assert.Equal(t, expectedStruct.Fields, resultDeserialized.GetData().GetStructValue().Fields)
}

func TestValueMapToProtocolBuffers_RecordWithInvalidType_PanicsWithError(t *testing.T) {
	record := Record{
		Key: "key1",
		Data: ValueMap{
			"first_name": String{Val: "foobar"},
			"age":        TestType{Val: 23}, // Invalid type
		},
	}

	expectedError := fmt.Sprintf(messaging.ErrConvertingToProtoStruct, "9999")

	assert.PanicsWithError(t, expectedError, func() {
		_, _ = messaging.ValueMapToProtocolBuffers(record.Data)
	}, "Expected panic with error: %s", expectedError)
}

func TestProtoStructToValueMap_ValidRecord_RecordShouldConvertToValueMap(t *testing.T) {
	protoStruct := structpb.Struct{
		Fields: map[string]*structpb.Value{
			"first_name": structpb.NewStringValue("foobar"),
			"last_name":  structpb.NewStringValue("random_lastname"),
			"age":        structpb.NewNumberValue(23),
		},
	}
	expectedStruct := ValueMap{
		"first_name": String{Val: "foobar"},
		"last_name":  String{Val: "random_lastname"},
		"age":        Integer{Val: 23},
	}

	result := messaging.ProtoStructToValueMap(&protoStruct)

	assert.Equal(t, expectedStruct, result)
}

func TestProtoStructToValueMap_InvalidProtoType_PanicsWithError(t *testing.T) {
	protoStruct := structpb.Struct{
		Fields: map[string]*structpb.Value{
			"first_name":    structpb.NewStringValue("foobar"),
			"invalid_field": {Kind: &structpb.Value_StructValue{}}, // Unsupported type
		},
	}

	expectedError := fmt.Sprintf(messaging.ErrConvertingToValueMap, "&{<nil>}")

	assert.PanicsWithError(t, expectedError, func() {
		_ = messaging.ProtoStructToValueMap(&protoStruct)
	}, "Expected panic with error: %s", expectedError)
}

func TestProtocolBuffersToValueMap_ValidRecord_ProtobufUnmarshalsToValueMap(t *testing.T) {
	protoStruct := &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"first_name": structpb.NewStringValue("foobar"),
			"last_name":  structpb.NewStringValue("random_lastname"),
			"age":        structpb.NewNumberValue(23),
		},
	}
	expectedStruct := ValueMap{
		"first_name": String{Val: "foobar"},
		"last_name":  String{Val: "random_lastname"},
		"age":        Integer{Val: 23},
	}

	value := structpb.NewStructValue(protoStruct)
	record := &messaging.RecordData{
		Data: value,
	}
	data, err := proto.Marshal(record)
	if err != nil {
		log.Fatalf("Failed to serialize: %v", err)
	}

	result := messaging.ProtocolBuffersToValueMap(data)
	assert.Equal(t, expectedStruct, result)
}

func TestProtocolBuffersToValueMap_InvalidProtoByte_Panics(t *testing.T) {
	invalidData := []byte{0xFF, 0x00, 0x01, 0x02}

	assert.Panics(t, func() {
		_ = messaging.ProtocolBuffersToValueMap(invalidData)
	}, "Expected panic when unmarshalling invalid protobuf data")
}

func TestProtocolBuffersSerialization_ValidRecord_ValueMapSerializesAndDeserializesEndToEnd(t *testing.T) {
	data := ValueMap{
		"first_name": String{Val: "foobar"},
		"last_name":  String{Val: "random_lastname"},
		"age":        Integer{Val: 23},
	}

	protoMessage, err := messaging.ValueMapToProtocolBuffers(data)
	assert.Nil(t, err)

	result := messaging.ProtocolBuffersToValueMap(protoMessage)
	assert.Equal(t, result, data)
}

func TestRecordToProtocolBuffers_ValidRecord_RecordShouldMarshalToProtobuf(t *testing.T) {
	now := time.Now()
	record := Record{
		Key: "key1",
		Data: ValueMap{
			"first_name": String{Val: "foobar"},
			"last_name":  String{Val: "random_lastname"},
			"age":        Integer{Val: 23},
		},
		Metadata: Metadata{
			Stream:    "test_stream",
			Timestamp: now,
		},
	}

	// Serialization
	result, err := messaging.RecordToProtocolBuffers(record)
	assert.Nil(t, err)

	// Deserialize the result
	var recordProto messaging.Record
	err = proto.Unmarshal(result, &recordProto)
	assert.Nil(t, err)

	// Check key
	assert.Equal(t, record.Key, recordProto.Key)

	// Check data
	expectedStruct := &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"first_name": structpb.NewStringValue("foobar"),
			"last_name":  structpb.NewStringValue("random_lastname"),
			"age":        structpb.NewNumberValue(23),
		},
	}
	assert.Equal(t, expectedStruct.Fields, recordProto.Data.Data.GetStructValue().Fields)

	// Check metadata
	assert.Equal(t, record.Metadata.Stream, recordProto.Metadata.Stream)
	assert.True(t, timestamppb.New(record.Metadata.Timestamp).AsTime().Equal(recordProto.Metadata.Timestamp.AsTime()))
}

func TestRecordToProtocolBuffers_InvalidData_PanicsWithError(t *testing.T) {
	record := Record{
		Key: "key1",
		Data: ValueMap{
			"first_name": String{Val: "foobar"},
			"age":        TestType{Val: 23}, // Invalid type
		},
		Metadata: Metadata{
			Stream:    "test_stream",
			Timestamp: time.Now(),
		},
	}

	expectedError := fmt.Sprintf(messaging.ErrConvertingToProtoStruct, "9999")

	assert.PanicsWithError(t, expectedError, func() {
		_, _ = messaging.RecordToProtocolBuffers(record)
	}, "Expected panic with error: %s", expectedError)
}

func TestProtocolBuffersToRecord_ValidProtobuf_ConvertsToRecord(t *testing.T) {
	now := time.Now()
	expectedRecord := Record{
		Key: "key1",
		Data: ValueMap{
			"first_name": String{Val: "foobar"},
			"last_name":  String{Val: "random_lastname"},
			"age":        Integer{Val: 23},
		},
		Metadata: Metadata{
			Stream:    "test_stream",
			Timestamp: now,
		},
	}

	// Create protobuf message representing the expected record
	protoStruct := &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"first_name": structpb.NewStringValue("foobar"),
			"last_name":  structpb.NewStringValue("random_lastname"),
			"age":        structpb.NewNumberValue(23),
		},
	}
	value := structpb.NewStructValue(protoStruct)
	recordData := &messaging.RecordData{
		Data: value,
	}
	recordProto := &messaging.Record{
		Key:  expectedRecord.Key,
		Data: recordData,
		Metadata: &messaging.Metadata{
			Stream:    expectedRecord.Metadata.Stream,
			Timestamp: timestamppb.New(expectedRecord.Metadata.Timestamp),
		},
	}

	data, err := proto.Marshal(recordProto)
	assert.Nil(t, err)

	// Assertions
	result, err := messaging.ProtocolBuffersToRecord(data)
	assert.Nil(t, err)
	assert.Equal(t, expectedRecord.Key, result.Key)
	assert.Equal(t, expectedRecord.Data, result.Data)
	assert.Equal(t, expectedRecord.Metadata.Stream, result.Metadata.Stream)
	assert.True(t, expectedRecord.Metadata.Timestamp.Equal(result.Metadata.Timestamp))
}

func TestProtocolBuffersToRecord_InvalidProtobuf_Panics(t *testing.T) {
	// Invalid byte slice (not a valid protobuf message)
	invalidData := []byte{0xFF, 0x00, 0x01, 0x02}

	result, err := messaging.ProtocolBuffersToRecord(invalidData)

	assert.Equal(t, Record{}, result)
	assert.Error(t, err)
}

func TestProtocolBuffersToRecord_InvalidDataField_PanicsWithError(t *testing.T) {
	// Prepare a protobuf message with an unsupported field type
	protoStruct := &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"invalid_field": {Kind: &structpb.Value_StructValue{}}, // Unsupported type
		},
	}
	value := structpb.NewStructValue(protoStruct)
	recordData := &messaging.RecordData{
		Data: value,
	}
	recordProto := &messaging.Record{
		Key:  "key1",
		Data: recordData,
		Metadata: &messaging.Metadata{
			Stream:    "test_stream",
			Timestamp: timestamppb.New(time.Now()),
		},
	}

	data, err := proto.Marshal(recordProto)
	assert.Nil(t, err)

	expectedError := fmt.Sprintf(messaging.ErrConvertingToValueMap, "&{}")

	assert.PanicsWithError(t, expectedError, func() {
		_, _ = messaging.ProtocolBuffersToRecord(data)
	}, "Expected panic with error: %s", expectedError)
}

func TestValueMapToProtoStruct_WithArray_ConvertsSuccessfully(t *testing.T) {
	record := Record{
		Key: "key1",
		Data: ValueMap{
			"numbers": Array{Val: []ColumnValue{
				Integer{Val: 1},
				Integer{Val: 2},
				Integer{Val: 3},
			}},
			"strings": Array{Val: []ColumnValue{
				String{Val: "a"},
				String{Val: "b"},
				String{Val: "c"},
			}},
		},
	}

	result, err := messaging.ValueMapToProtoStruct(record.Data)
	assert.Nil(t, err)

	expectedStruct := structpb.Struct{
		Fields: map[string]*structpb.Value{
			"numbers": structpb.NewListValue(&structpb.ListValue{
				Values: []*structpb.Value{
					structpb.NewNumberValue(1),
					structpb.NewNumberValue(2),
					structpb.NewNumberValue(3),
				},
			}),
			"strings": structpb.NewListValue(&structpb.ListValue{
				Values: []*structpb.Value{
					structpb.NewStringValue("a"),
					structpb.NewStringValue("b"),
					structpb.NewStringValue("c"),
				},
			}),
		},
	}

	assert.Equal(t, expectedStruct.AsMap(), result.AsMap())
}

func TestProtoStructToValueMap_WithArray_ConvertsSuccessfully(t *testing.T) {
	protoStruct := &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"numbers": structpb.NewListValue(&structpb.ListValue{
				Values: []*structpb.Value{
					structpb.NewNumberValue(1),
					structpb.NewNumberValue(2),
					structpb.NewNumberValue(3),
				},
			}),
			"strings": structpb.NewListValue(&structpb.ListValue{
				Values: []*structpb.Value{
					structpb.NewStringValue("a"),
					structpb.NewStringValue("b"),
					structpb.NewStringValue("c"),
				},
			}),
		},
	}

	expectedValueMap := ValueMap{
		"numbers": Array{Val: []ColumnValue{
			Integer{Val: 1},
			Integer{Val: 2},
			Integer{Val: 3},
		}},
		"strings": Array{Val: []ColumnValue{
			String{Val: "a"},
			String{Val: "b"},
			String{Val: "c"},
		}},
	}

	result := messaging.ProtoStructToValueMap(protoStruct)

	assert.Equal(t, expectedValueMap, result)
}

func TestRecordToProtocolBuffers_WithArray_SerializesAndDeserializesSuccessfully(t *testing.T) {
	now := time.Now()
	record := Record{
		Key: "key_with_array",
		Data: ValueMap{
			"numbers": Array{Val: []ColumnValue{
				Integer{Val: 10},
				Integer{Val: 20},
				Integer{Val: 30},
			}},
			"nested_array": Array{Val: []ColumnValue{
				Array{Val: []ColumnValue{
					String{Val: "nested1"},
					String{Val: "nested2"},
				}},
				String{Val: "outside"},
			}},
		},
		Metadata: Metadata{
			Stream:    "array_stream",
			Timestamp: now,
		},
	}

	// Serialize the record
	serializedData, err := messaging.RecordToProtocolBuffers(record)
	assert.Nil(t, err)

	// Deserialize the record
	deserializedRecord, err := messaging.ProtocolBuffersToRecord(serializedData)
	assert.Nil(t, err)

	// Assertions
	assert.Equal(t, record.Key, deserializedRecord.Key)
	assert.Equal(t, record.Metadata.Stream, deserializedRecord.Metadata.Stream)
	assert.True(t, record.Metadata.Timestamp.Equal(deserializedRecord.Metadata.Timestamp))

	// Compare Data
	assert.True(t, reflect.DeepEqual(record.Data, deserializedRecord.Data))
}

func TestValueMapToProtoStruct_ArrayWithInvalidType_PanicsWithError(t *testing.T) {
	record := Record{
		Data: ValueMap{
			"invalid_array": Array{Val: []ColumnValue{
				TestType{Val: 123}, // Invalid type inside array
			}},
		},
	}

	expectedError := fmt.Sprintf(messaging.ErrConvertingToProtoStruct, "9999")

	assert.PanicsWithError(t, expectedError, func() {
		_, _ = messaging.ValueMapToProtoStruct(record.Data)
	}, "Expected panic with error: %s", expectedError)
}

func TestColumnValueToProtoValue_WithInteger_ConvertsSuccessfully(t *testing.T) {
	value := Integer{Val: 123}

	result, err := messaging.ColumnValueToProtoValue(value)
	assert.Nil(t, err)

	expectedResult := structpb.NewNumberValue(123)

	assert.Equal(t, expectedResult, result)
}

func TestColumnValueToProtoValue_WithString_ConvertsSuccessfully(t *testing.T) {
	value := String{Val: "hello"}

	result, err := messaging.ColumnValueToProtoValue(value)
	assert.Nil(t, err)

	expectedResult := structpb.NewStringValue("hello")

	assert.Equal(t, expectedResult, result)
}

func TestColumnValueToProtoValue_WithArrayOfIntegers_ConvertsSuccessfully(t *testing.T) {
	value := Array{Val: []ColumnValue{
		Integer{Val: 1},
		Integer{Val: 2},
		Integer{Val: 3},
	}}

	result, err := messaging.ColumnValueToProtoValue(value)
	assert.Nil(t, err)

	expectedResult := structpb.NewListValue(&structpb.ListValue{
		Values: []*structpb.Value{
			structpb.NewNumberValue(1),
			structpb.NewNumberValue(2),
			structpb.NewNumberValue(3),
		},
	})

	assert.Equal(t, expectedResult, result)
}

func TestColumnValueToProtoValue_WithArrayOfStrings_ConvertsSuccessfully(t *testing.T) {
	value := Array{Val: []ColumnValue{
		String{Val: "a"},
		String{Val: "b"},
		String{Val: "c"},
	}}

	result, err := messaging.ColumnValueToProtoValue(value)
	assert.Nil(t, err)

	expectedResult := structpb.NewListValue(&structpb.ListValue{
		Values: []*structpb.Value{
			structpb.NewStringValue("a"),
			structpb.NewStringValue("b"),
			structpb.NewStringValue("c"),
		},
	})

	assert.Equal(t, expectedResult, result)
}

func TestColumnValueToProtoValue_WithUnsupportedType_Panics(t *testing.T) {
	unsupportedValue := TestType{Val: -100}

	errorValue := fmt.Sprintf(messaging.ErrConvertingToProtoStruct, "9999")

	assert.PanicsWithError(t, errorValue, func() {
		_, _ = messaging.ColumnValueToProtoValue(unsupportedValue)
	})
}

func TestColumnValueToProtoValue_WithNestedArray_ConvertsSuccessfully(t *testing.T) {
	value := Array{Val: []ColumnValue{
		Array{Val: []ColumnValue{
			Integer{Val: 1},
			Integer{Val: 2},
		}},
		Array{Val: []ColumnValue{
			String{Val: "a"},
			String{Val: "b"},
		}},
	}}

	result, err := messaging.ColumnValueToProtoValue(value)
	assert.Nil(t, err)

	expectedResult := structpb.NewListValue(&structpb.ListValue{
		Values: []*structpb.Value{
			structpb.NewListValue(&structpb.ListValue{
				Values: []*structpb.Value{
					structpb.NewNumberValue(1),
					structpb.NewNumberValue(2),
				},
			}),
			structpb.NewListValue(&structpb.ListValue{
				Values: []*structpb.Value{
					structpb.NewStringValue("a"),
					structpb.NewStringValue("b"),
				},
			}),
		},
	})

	assert.Equal(t, expectedResult, result)
}

func TestProtoValueToColumnValue_WithStringValue_ConvertsSuccessfully(t *testing.T) {
	stringValue := structpb.NewStringValue("test")

	result := messaging.ProtoValueToColumnValue(stringValue)

	expectedResult := String{Val: "test"}
	assert.Equal(t, expectedResult, result)
}

func TestProtoValueToColumnValue_WithNumberValue_ConvertsSuccessfully(t *testing.T) {
	numberValue := structpb.NewNumberValue(42)

	result := messaging.ProtoValueToColumnValue(numberValue)

	expectedResult := Integer{Val: 42}
	assert.Equal(t, expectedResult, result)
}

func TestProtoValueToColumnValue_WithArrayOfMixedValues_ConvertsSuccessfully(t *testing.T) {
	listValue := structpb.NewListValue(&structpb.ListValue{
		Values: []*structpb.Value{
			structpb.NewStringValue("abc"),
			structpb.NewNumberValue(123),
			structpb.NewStringValue("xyz"),
		},
	})

	result := messaging.ProtoValueToColumnValue(listValue)

	expectedResult := Array{Val: []ColumnValue{
		String{Val: "abc"},
		Integer{Val: 123},
		String{Val: "xyz"},
	}}
	assert.Equal(t, expectedResult, result)
}

func TestProtoValueToColumnValue_WithUnsupportedType_Panics(t *testing.T) {
	unsupportedValue := structpb.NewNullValue()

	errorValue := fmt.Sprintf(messaging.ErrConvertingToValueMap, "&{NULL_VALUE}")

	assert.PanicsWithError(t, errorValue, func() {
		_ = messaging.ProtoValueToColumnValue(unsupportedValue)
	})
}

func TestProtoValueToColumnValue_WithNestedArrayOfValues_ConvertsSuccessfully(t *testing.T) {
	nestedListValue := structpb.NewListValue(&structpb.ListValue{
		Values: []*structpb.Value{
			structpb.NewListValue(&structpb.ListValue{
				Values: []*structpb.Value{
					structpb.NewStringValue("nested1"),
					structpb.NewNumberValue(99),
				},
			}),
			structpb.NewStringValue("outer"),
		},
	})

	result := messaging.ProtoValueToColumnValue(nestedListValue)

	expectedResult := Array{Val: []ColumnValue{
		Array{Val: []ColumnValue{
			String{Val: "nested1"},
			Integer{Val: 99},
		}},
		String{Val: "outer"},
	}}
	assert.Equal(t, expectedResult, result)
}

func TestProtoValueToColumnValue_WithEmptyList_ConvertsSuccessfully(t *testing.T) {
	emptyListValue := structpb.NewListValue(&structpb.ListValue{
		Values: []*structpb.Value{},
	})

	result := messaging.ProtoValueToColumnValue(emptyListValue)

	expectedResult := Array{Val: []ColumnValue{}}
	assert.Equal(t, expectedResult, result)
}
