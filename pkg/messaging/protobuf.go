package messaging

import (
	"fmt"

	"github.com/farbodahm/streame/pkg/types"
	"github.com/farbodahm/streame/pkg/utils"
	"google.golang.org/protobuf/types/known/structpb"
)

var ErrConvertingToProtoStruct = "Failed converting to Proto struct, unsupported ColumnType: '%v'"

// RecordToProtoStruct converts data of given record to google.protobuf.Struct
func RecordToProtoStruct(record types.Record) (structpb.Struct, error) {
	fields := make(map[string]*structpb.Value)
	for k, v := range record.Data {
		var pbValue *structpb.Value

		switch v.Type() {
		case types.IntType:
			pbValue = structpb.NewNumberValue(float64(v.ToInt()))
		case types.StringType:
			pbValue = structpb.NewStringValue(v.ToString())
		default:
			utils.Logger.Error("Failed to convert record to protubuf struct", "id", record.Key)
			return structpb.Struct{}, fmt.Errorf(ErrConvertingToProtoStruct, v.Type())
		}

		fields[k] = pbValue
	}

	return structpb.Struct{Fields: fields}, nil
}
