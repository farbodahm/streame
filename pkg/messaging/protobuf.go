package messaging

import (
	"fmt"

	"github.com/farbodahm/streame/pkg/types"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

var ErrConvertingToProtoStruct = "Failed converting to Proto struct, unsupported ColumnType: '%v'"
var ErrConvertingToValueMap = "Failed converting to ValueMap, unsupported ColumnType: '%v'"

// ValueMapToProtoStruct converts data of given record to google.protobuf.Struct
func ValueMapToProtoStruct(data types.ValueMap) (structpb.Struct, error) {
	fields := make(map[string]*structpb.Value)
	for k, v := range data {
		var pbValue *structpb.Value

		switch v.Type() {
		case types.IntType:
			pbValue = structpb.NewNumberValue(float64(v.ToInt()))
		case types.StringType:
			pbValue = structpb.NewStringValue(v.ToString())
		default:
			return structpb.Struct{}, fmt.Errorf(ErrConvertingToProtoStruct, v.Type())
		}

		fields[k] = pbValue
	}

	return structpb.Struct{Fields: fields}, nil
}

// ValueMapToProtocolBuffers marshals given record to protocol buffers
func ValueMapToProtocolBuffers(data types.ValueMap) ([]byte, error) {
	protoStruct, err := ValueMapToProtoStruct(data)
	if err != nil {
		return nil, err
	}

	valueData := structpb.NewStructValue(&protoStruct)
	recordMessage := &RecordData{
		Data: valueData,
	}

	return proto.Marshal(recordMessage)
}

// ProtoStructToValueMap converts google.protobuf.Struct to a ValueMap
// This is opposite of ValueMapToProtoStruct
func ProtoStructToValueMap(protoStruct *structpb.Struct) (types.ValueMap, error) {
	data := types.ValueMap{}
	for k, v := range protoStruct.Fields {
		switch v.GetKind().(type) {
		case *structpb.Value_NumberValue:
			// FIXME: identify the type correctly
			data[k] = types.Integer{Val: int(v.GetNumberValue())}
		case *structpb.Value_StringValue:
			data[k] = types.String{Val: v.GetStringValue()}
		default:
			return types.ValueMap{}, fmt.Errorf(
				ErrConvertingToValueMap,
				v.GetKind(),
			)
		}
	}

	return data, nil
}

// ProtocolBuffersToValueMap unmarshals protocol buffers data to a ValueMap
func ProtocolBuffersToValueMap(data []byte) (types.ValueMap, error) {
	recordMessage := RecordData{}
	err := proto.Unmarshal(data, &recordMessage)
	if err != nil {
		return nil, err
	}

	return ProtoStructToValueMap(recordMessage.Data.GetStructValue())
}
