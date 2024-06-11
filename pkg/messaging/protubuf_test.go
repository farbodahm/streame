package messaging_test

import (
	"testing"

	"github.com/farbodahm/streame/pkg/messaging"
	. "github.com/farbodahm/streame/pkg/types"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestRecordToProtoStruct_ValidRecord_RecordShouldConvertToProtobufStruct(t *testing.T) {
	record := Record{
		Key: "key1",
		Data: ValueMap{
			"first_name": String{Val: "foobar"},
			"last_name":  String{Val: "random_lastname"},
			"age":        Integer{Val: 23},
		},
	}

	result, err := messaging.RecordToProtoStruct(record)
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
