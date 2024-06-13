package messaging_test

import (
	"fmt"
	"testing"

	"github.com/farbodahm/streame/pkg/messaging"
	. "github.com/farbodahm/streame/pkg/types"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
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

func TestRecordDataToProtoStruct_ValidRecord_RecordShouldConvertToProtobufStruct(t *testing.T) {
	record := Record{
		Key: "key1",
		Data: ValueMap{
			"first_name": String{Val: "foobar"},
			"last_name":  String{Val: "random_lastname"},
			"age":        Integer{Val: 23},
		},
	}

	result, err := messaging.RecordDataToProtoStruct(record.Data)
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

func TestRecordDataToProtoStruct_RecordWithInvalidType_ReturnErrorConvertingToProtoStruct(t *testing.T) {
	record := Record{
		Key: "key1",
		Data: ValueMap{
			"first_name": String{Val: "foobar"},
			"last_name":  String{Val: "random_lastname"},
			"age":        TestType{Val: 23},
		},
	}

	result, err := messaging.RecordDataToProtoStruct(record.Data)
	expected_error := fmt.Sprintf(messaging.ErrConvertingToProtoStruct, "9999")

	assert.EqualError(t, err, expected_error)
	assert.Equal(t, structpb.Struct{}.Fields, result.Fields)
}

func TestRecordDataToProtocolBuffers_ValidRecord_RecordMarshalsToProtobuf(t *testing.T) {
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

	result, err := messaging.RecordDataToProtocolBuffers(record.Data)
	assert.Nil(t, err)

	var resultDeserialized messaging.RecordData
	err = proto.Unmarshal(result, &resultDeserialized)
	assert.Nil(t, err)
	assert.Equal(t, expected_struct.Fields, resultDeserialized.GetData().GetStructValue().Fields)
}

func TestRecordDataToProtocolBuffers_RecordWithInvalidType_ReturnErrorConvertingToProtoStruct(t *testing.T) {
	record := Record{
		Key: "key1",
		Data: ValueMap{
			"first_name": String{Val: "foobar"},
			"last_name":  String{Val: "random_lastname"},
			"age":        TestType{Val: 23},
		},
	}

	result, err := messaging.RecordDataToProtocolBuffers(record.Data)
	expected_error := fmt.Sprintf(messaging.ErrConvertingToProtoStruct, "9999")

	assert.EqualError(t, err, expected_error)
	assert.Nil(t, result)
}
