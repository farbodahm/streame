package messaging

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/farbodahm/streame/pkg/types"
	"google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	status "google.golang.org/grpc/status"
)

// RecordStreamService implements the RecordStream GRPC service.
// It receives records from a channel and streams them to the client.
type RecordStreamService struct {
	UnimplementedRecordStreamServer

	InputChan <-chan types.Record
}

func (s *RecordStreamService) StreamRecords(req *StreamRequest, stream RecordStream_StreamRecordsServer) error {
	slog.Info("Client subscribed to stream", "node", req.GetNodeId())

	for {
		select {
		case <-stream.Context().Done():
			slog.Info("Client disconnected or context cancelled")
			return stream.Context().Err()

		// TODO(STR-001): Correctly assign the records to nodes; Currently first client reading from the channel
		// will receive the records.
		case record, ok := <-s.InputChan:
			if !ok {
				slog.Info("Record channel closed. Ending stream.")
				return nil
			}

			protoRecord, err := RecordToProtocolBuffersRecord(record)
			if err != nil {
				slog.Error("Error converting record to protocol buffers", "error", err)
				return err
			}

			if err := stream.Send(protoRecord); err != nil {
				slog.Error("Error sending record", "error", err)
				return err
			}
		}
	}
}

// StreamRecordsFromLeader connects to the leader's gRPC RecordStream service
// and returns channels for receiving types.Record and errors.
func StreamRecordsFromLeader(ctx context.Context, address, nodeID string) (chan types.Record, <-chan error) {

	recChan := make(chan types.Record)
	errChan := make(chan error, 1)
	go func() {
		defer close(recChan)
		defer close(errChan)

		// TODO: Use TLS credentials in production
		conn, err := grpc.NewClient(
			address,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			errChan <- fmt.Errorf("failed to create gRPC client: %w", err)
			return
		}
		defer conn.Close()

		client := NewRecordStreamClient(conn)
		stream, err := client.StreamRecords(ctx, &StreamRequest{NodeId: nodeID})
		if err != nil {
			errChan <- fmt.Errorf("error starting stream: %w", err)
			return
		}

		for {
			msg, err := stream.Recv()
			if err != nil {
				// TODO: Safely check if error is because the stream was closed
				if status.Code(err) == codes.Unavailable {
					slog.Warn("Stream cancelled by leader")
					return
				}
				errChan <- fmt.Errorf("error receiving from stream: %w", err)
				return
			}

			record, err := ProtocolBuffersRecordToRecord(msg)
			if err != nil {
				errChan <- fmt.Errorf("error converting record: %w", err)
				return
			}
			recChan <- record
		}
	}()
	return recChan, errChan
}
