package benchmarks

import (
	"testing"

	"github.com/farbodahm/streame/pkg/messaging"
	"github.com/farbodahm/streame/pkg/utils"
)

// serializeAndDeserializeRecords serializes and deserializes a set number of records
func serializeAndDeserializeRecords(number_of_records int) {
	for i := 0; i < number_of_records; i++ {
		record := utils.NewHeavyRecord(100)

		// Serialize the record to protobuf
		data, err := messaging.RecordToProtocolBuffers(record)
		if err != nil {
			panic(err)
		}

		// Deserialize the protobuf back to a record
		_, err = messaging.ProtocolBuffersToRecord(data)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkProtobufSerialization(b *testing.B) {
	for i := 0; i < b.N; i++ {
		serializeAndDeserializeRecords(1000)
	}
}
