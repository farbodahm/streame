package messaging

import (
	"fmt"

	"github.com/farbodahm/streame/pkg/types"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var ErrConvertingToProtoStruct = "Failed converting to Proto struct, unsupported ColumnType: '%v'"
var ErrConvertingToValueMap = "Failed converting to ValueMap, unsupported Value kind: '%v'"

// Helper function to convert ColumnValue to *structpb.Value
func columnValueToProtoValue(v types.ColumnValue) (*structpb.Value, error) {
	switch v.Type() {
	case types.IntType:
		return structpb.NewNumberValue(float64(v.ToInt())), nil
	case types.StringType:
		return structpb.NewStringValue(v.ToString()), nil
	case types.ArrayType:
		arrValues := v.ToArray()
		pbListValues := make([]*structpb.Value, len(arrValues))
		for i, elem := range arrValues {
			elemPbValue, err := columnValueToProtoValue(elem)
			if err != nil {
				return nil, err
			}
			pbListValues[i] = elemPbValue
		}
		return structpb.NewListValue(&structpb.ListValue{Values: pbListValues}), nil
	default:
		panic(fmt.Errorf(ErrConvertingToProtoStruct, v.Type()))
	}
}

// ValueMapToProtoStruct converts data of given record to google.protobuf.Struct
func ValueMapToProtoStruct(data types.ValueMap) (structpb.Struct, error) {
	fields := make(map[string]*structpb.Value)
	for k, v := range data {
		pbValue, err := columnValueToProtoValue(v)
		if err != nil {
			return structpb.Struct{}, err
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

// Helper function to convert *structpb.Value to ColumnValue
func protoValueToColumnValue(v *structpb.Value) types.ColumnValue {
	switch kind := v.GetKind().(type) {
	case *structpb.Value_NumberValue:
		return types.Integer{Val: int(kind.NumberValue)}
	case *structpb.Value_StringValue:
		return types.String{Val: kind.StringValue}
	case *structpb.Value_ListValue:
		pbListValues := kind.ListValue.GetValues()
		arrValues := make([]types.ColumnValue, len(pbListValues))
		for i, pbValue := range pbListValues {
			arrValues[i] = protoValueToColumnValue(pbValue)
		}
		return types.Array{Val: arrValues}
	default:
		panic(fmt.Errorf(ErrConvertingToValueMap, v.GetKind()))
	}
}

// ProtoStructToValueMap converts google.protobuf.Struct to a ValueMap
func ProtoStructToValueMap(protoStruct *structpb.Struct) types.ValueMap {
	data := types.ValueMap{}
	for k, v := range protoStruct.Fields {
		columnValue := protoValueToColumnValue(v)
		data[k] = columnValue
	}
	return data
}

// ProtocolBuffersToValueMap unmarshals protocol buffers data to a ValueMap
func ProtocolBuffersToValueMap(data []byte) types.ValueMap {
	recordMessage := RecordData{}
	err := proto.Unmarshal(data, &recordMessage)
	if err != nil {
		panic(err)
	}

	return ProtoStructToValueMap(recordMessage.Data.GetStructValue())
}

// RecordToProtocolBuffers serializes a single record to Protobuf message
func RecordToProtocolBuffers(record types.Record) ([]byte, error) {
	// Data field
	protoStruct, err := ValueMapToProtoStruct(record.Data)
	if err != nil {
		return nil, err
	}
	valueData := structpb.NewStructValue(&protoStruct)
	recordData := &RecordData{
		Data: valueData,
	}

	// Metadata field
	metadataProto := &Metadata{
		Stream:    record.Metadata.Stream,
		Timestamp: timestamppb.New(record.Metadata.Timestamp),
	}

	recordProto := &Record{
		Key:      record.Key,
		Data:     recordData,
		Metadata: metadataProto,
	}

	return proto.Marshal(recordProto)
}

// ProtocolBuffersToRecord unmarshals protocol buffer data into a Record
func ProtocolBuffersToRecord(data []byte) (types.Record, error) {
	var recordProto Record
	err := proto.Unmarshal(data, &recordProto)
	if err != nil {
		return types.Record{}, err
	}

	// Convert Data field
	valueMap := ProtoStructToValueMap(recordProto.Data.Data.GetStructValue())

	// Convert Metadata field
	metadata := types.Metadata{
		Stream:    recordProto.Metadata.Stream,
		Timestamp: recordProto.Metadata.Timestamp.AsTime(),
	}

	// Create and return the Record
	record := types.Record{
		Key:      recordProto.Key,
		Data:     valueMap,
		Metadata: metadata,
	}

	return record, nil
}

// FIXME: Add an issue to change IntegerValue to NumberValue and change the underlying data type to float64
// FIXME: Add an optimization issue to verify how serialization/deserialization to and from Protobuf is now bad with recursive nature.
