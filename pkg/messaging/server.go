package messaging

import (
	"log"

	"github.com/farbodahm/streame/pkg/types"
)

// RecordStreamService implements the RecordStream GRPC service.
// It receives records from a channel and streams them to the client.
type RecordStreamService struct {
	UnimplementedRecordStreamServer

	InputChan <-chan types.Record
}

func (s *RecordStreamService) StreamRecords(req *StreamRequest, stream RecordStream_StreamRecordsServer) error {
	log.Printf("Client subscribed to stream: %s\n", req.GetNodeId())

	for {
		select {
		case <-stream.Context().Done():
			log.Println("Client disconnected or context cancelled")
			return stream.Context().Err()

		case record, ok := <-s.InputChan:
			if !ok {
				log.Println("Record channel closed. Ending stream.")
				return nil
			}

			protoRecord, err := RecordToProtocolBuffersRecord(record)
			if err != nil {
				log.Printf("Error converting record to protocol buffers: %v", err)
				return err
			}

			if err := stream.Send(protoRecord); err != nil {
				log.Printf("Error sending record: %v", err)
				return err
			}
		}
	}
}
