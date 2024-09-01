package messaging_test

import (
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/farbodahm/streame/pkg/messaging"
	. "github.com/farbodahm/streame/pkg/types"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Make sure TestType implements ColumnValue
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
	expected_struct := structpb.Struct{
		Fields: map[string]*structpb.Value{
			"first_name": {Kind: &structpb.Value_StringValue{StringValue: "foobar"}},
			"last_name":  {Kind: &structpb.Value_StringValue{StringValue: "random_lastname"}},
			"age":        {Kind: &structpb.Value_NumberValue{NumberValue: 23}},
		},
	}

	assert.Nil(t, err)
	assert.Equal(t, expected_struct.AsMap(), result.AsMap())
}

func TestValueMapToProtoStruct_RecordWithInvalidType_ReturnErrorConvertingToProtoStruct(t *testing.T) {
	record := Record{
		Key: "key1",
		Data: ValueMap{
			"first_name": String{Val: "foobar"},
			"last_name":  String{Val: "random_lastname"},
			"age":        TestType{Val: 23},
		},
	}

	result, err := messaging.ValueMapToProtoStruct(record.Data)
	expected_error := fmt.Sprintf(messaging.ErrConvertingToProtoStruct, "9999")

	assert.EqualError(t, err, expected_error)
	assert.Equal(t, structpb.Struct{}.Fields, result.Fields)
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
	expected_struct := structpb.Struct{
		Fields: map[string]*structpb.Value{
			"first_name": {Kind: &structpb.Value_StringValue{StringValue: "foobar"}},
			"last_name":  {Kind: &structpb.Value_StringValue{StringValue: "random_lastname"}},
			"age":        {Kind: &structpb.Value_NumberValue{NumberValue: 23}},
		},
	}

	result, err := messaging.ValueMapToProtocolBuffers(record.Data)
	assert.Nil(t, err)

	var resultDeserialized messaging.RecordData
	err = proto.Unmarshal(result, &resultDeserialized)
	assert.Nil(t, err)
	assert.Equal(t, expected_struct.Fields, resultDeserialized.GetData().GetStructValue().Fields)
}

func TestValueMapToProtocolBuffers_RecordWithInvalidType_ReturnErrorConvertingToProtoStruct(t *testing.T) {
	record := Record{
		Key: "key1",
		Data: ValueMap{
			"first_name": String{Val: "foobar"},
			"last_name":  String{Val: "random_lastname"},
			"age":        TestType{Val: 23},
		},
	}

	result, err := messaging.ValueMapToProtocolBuffers(record.Data)
	expected_error := fmt.Sprintf(messaging.ErrConvertingToProtoStruct, "9999")

	assert.EqualError(t, err, expected_error)
	assert.Nil(t, result)
}

func TestProtoStructToValueMap_ValidRecord_RecordShouldConvertToValueMap(t *testing.T) {
	protoStruct := structpb.Struct{
		Fields: map[string]*structpb.Value{
			"first_name": {Kind: &structpb.Value_StringValue{StringValue: "foobar"}},
			"last_name":  {Kind: &structpb.Value_StringValue{StringValue: "random_lastname"}},
			"age":        {Kind: &structpb.Value_NumberValue{NumberValue: 23}},
		},
	}
	expected_struct := ValueMap{
		"first_name": String{Val: "foobar"},
		"last_name":  String{Val: "random_lastname"},
		"age":        Integer{Val: 23},
	}

	result, err := messaging.ProtoStructToValueMap(&protoStruct)

	assert.Nil(t, err)
	assert.Equal(t, expected_struct, result)
}

func TestProtoStructToValueMap_InvalidProtoType_ReturnErrorConvertingToValueMap(t *testing.T) {
	protoStruct := structpb.Struct{
		Fields: map[string]*structpb.Value{
			"first_name":    {Kind: &structpb.Value_StringValue{StringValue: "foobar"}},
			"last_name":     {Kind: &structpb.Value_StringValue{StringValue: "random_lastname"}},
			"invalid_field": {Kind: &structpb.Value_StructValue{}},
		},
	}
	expected_error := fmt.Sprintf(messaging.ErrConvertingToValueMap, "&{<nil>}")

	result, err := messaging.ProtoStructToValueMap(&protoStruct)

	assert.EqualError(t, err, expected_error)
	assert.Equal(t, ValueMap{}, result)
}

func TestProtocolBuffersToValueMap_ValidRecord_ProtobufUnmarshalsToValueMap(t *testing.T) {
	protoStruct := &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"first_name": structpb.NewStringValue("foobar"),
			"last_name":  structpb.NewStringValue("random_lastname"),
			"age":        structpb.NewNumberValue(23),
		},
	}
	expected_struct := ValueMap{
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

	result, err := messaging.ProtocolBuffersToValueMap(data)
	assert.Nil(t, err)
	assert.Equal(t, expected_struct, result)
}

func TestProtocolBuffersToValueMap_InValidProtoByte_ReturnErr(t *testing.T) {
	invalidData := []byte{0xFF, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F}

	result, err := messaging.ProtocolBuffersToValueMap(invalidData)

	assert.NotNil(t, err)
	assert.Nil(t, result)
}

func TestProtocolBuffersSerialization_ValidRecord_ValeMapSerializesAndDeserializesEndToEnd(t *testing.T) {
	data := ValueMap{
		"first_name": String{Val: "foobar"},
		"last_name":  String{Val: "random_lastname"},
		"age":        Integer{Val: 23},
	}

	protoMessage, err := messaging.ValueMapToProtocolBuffers(data)
	assert.Nil(t, err)

	result, err := messaging.ProtocolBuffersToValueMap(protoMessage)

	assert.Nil(t, err)
	assert.Equal(t, result, data)
}

func TestRecordToProtocolBuffers_ValidRecord_RecordShouldMarshalToProtobuf(t *testing.T) {
	record := Record{
		Key: "key1",
		Data: ValueMap{
			"first_name": String{Val: "foobar"},
			"last_name":  String{Val: "random_lastname"},
			"age":        Integer{Val: 23},
		},
		Metadata: Metadata{
			Stream:    "test_stream",
			Timestamp: time.Now(),
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
	expectedStruct := structpb.Struct{
		Fields: map[string]*structpb.Value{
			"first_name": {Kind: &structpb.Value_StringValue{StringValue: "foobar"}},
			"last_name":  {Kind: &structpb.Value_StringValue{StringValue: "random_lastname"}},
			"age":        {Kind: &structpb.Value_NumberValue{NumberValue: 23}},
		},
	}
	assert.Equal(t, expectedStruct.Fields, recordProto.Data.Data.GetStructValue().Fields)

	// Check metadata
	assert.Equal(t, record.Metadata.Stream, recordProto.Metadata.Stream)
	assert.Equal(t, timestamppb.New(record.Metadata.Timestamp), recordProto.Metadata.Timestamp)
}

func TestRecordToProtocolBuffers_InvalidData_ReturnError(t *testing.T) {
	record := Record{
		Key: "key1",
		Data: ValueMap{
			"first_name": String{Val: "foobar"},
			"last_name":  String{Val: "random_lastname"},
			"age":        TestType{Val: 23}, // Invalid type
		},
		Metadata: Metadata{
			Stream:    "test_stream",
			Timestamp: time.Now(),
		},
	}

	// Call the function
	result, err := messaging.RecordToProtocolBuffers(record)
	expectedError := fmt.Sprintf(messaging.ErrConvertingToProtoStruct, "9999")

	assert.EqualError(t, err, expectedError)
	assert.Nil(t, result)
}
