package messaging

import (
	"fmt"

	"github.com/farbodahm/streame/pkg/types"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
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
	valueMap, err := ProtoStructToValueMap(recordProto.Data.Data.GetStructValue())
	if err != nil {
		return types.Record{}, fmt.Errorf("error converting Data field: %w", err)
	}

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
