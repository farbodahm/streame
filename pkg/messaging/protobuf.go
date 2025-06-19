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

// ColumnValueToProtoValue converts ColumnValue to *structpb.Value
func ColumnValueToProtoValue(v types.ColumnValue) (*structpb.Value, error) {
	switch v.Type() {
	case types.IntType:
		return structpb.NewNumberValue(float64(v.ToInt())), nil
	case types.StringType:
		return structpb.NewStringValue(v.ToString()), nil
	case types.ArrayType:
		arrValues := v.ToArray()
		pbListValues := make([]*structpb.Value, len(arrValues))
		for i, elem := range arrValues {
			elemPbValue, err := ColumnValueToProtoValue(elem)
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
		pbValue, err := ColumnValueToProtoValue(v)
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

// ProtoValueToColumnValue converts *structpb.Value to ColumnValue
func ProtoValueToColumnValue(v *structpb.Value) types.ColumnValue {
	switch kind := v.GetKind().(type) {
	case *structpb.Value_NumberValue:
		return types.Integer{Val: int(kind.NumberValue)}
	case *structpb.Value_StringValue:
		return types.String{Val: kind.StringValue}
	case *structpb.Value_ListValue:
		pbListValues := kind.ListValue.GetValues()
		arrValues := make([]types.ColumnValue, len(pbListValues))
		for i, pbValue := range pbListValues {
			arrValues[i] = ProtoValueToColumnValue(pbValue)
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
		columnValue := ProtoValueToColumnValue(v)
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

// RecordToProtocolBuffersRecord converts a types.Record to a protobuf Record message.
func RecordToProtocolBuffersRecord(record types.Record) (*Record, error) {
	protoStruct, err := ValueMapToProtoStruct(record.Data)
	if err != nil {
		return nil, err
	}
	recordData := &RecordData{
		Data: structpb.NewStructValue(&protoStruct),
	}
	metadataProto := &Metadata{
		Stream:    record.Metadata.Stream,
		Timestamp: timestamppb.New(record.Metadata.Timestamp),
	}
	recordProto := &Record{
		Key:      record.Key,
		Data:     recordData,
		Metadata: metadataProto,
	}
	return recordProto, nil
}

// ProtocolBuffersRecordToRecord converts a protobuf Record message to a types.Record.
func ProtocolBuffersRecordToRecord(recordProto *Record) (types.Record, error) {
	valueMap := ProtoStructToValueMap(recordProto.Data.Data.GetStructValue())
	metadata := types.Metadata{
		Stream:    recordProto.Metadata.Stream,
		Timestamp: recordProto.Metadata.Timestamp.AsTime(),
	}
	record := types.Record{
		Key:      recordProto.Key,
		Data:     valueMap,
		Metadata: metadata,
	}
	return record, nil
}

// RecordToProtocolBuffers serializes a single record to Protobuf message
func RecordToProtocolBuffers(record types.Record) ([]byte, error) {
	recordProto, err := RecordToProtocolBuffersRecord(record)
	if err != nil {
		return nil, err
	}
	return proto.Marshal(recordProto)
}

// ProtocolBuffersToRecord unmarshals protocol buffer data into a Record
func ProtocolBuffersToRecord(data []byte) (types.Record, error) {
	var recordProto Record
	if err := proto.Unmarshal(data, &recordProto); err != nil {
		return types.Record{}, err
	}
	return ProtocolBuffersRecordToRecord(&recordProto)
}
