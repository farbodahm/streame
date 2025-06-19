package messaging

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/farbodahm/streame/pkg/types"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// fakeStream implements grpc.ServerStream and RecordStream_StreamRecordsServer for testing.
type fakeStream struct {
	grpc.ServerStream
	ctx context.Context
	Ch  chan *Record
}

func (f *fakeStream) Context() context.Context     { return f.ctx }
func (f *fakeStream) Send(r *Record) error         { f.Ch <- r; return nil }
func (f *fakeStream) SetHeader(metadata.MD) error  { return nil }
func (f *fakeStream) SendHeader(metadata.MD) error { return nil }
func (f *fakeStream) SetTrailer(metadata.MD)       {}
func (f *fakeStream) SendMsg(m any) error          { return nil }
func (f *fakeStream) RecvMsg(m any) error          { return nil }

func TestRecordStreamService_StreamRecords_SendRecords(t *testing.T) {
	// Prepare input records
	now := time.Now()
	recs := []types.Record{
		{Key: "k1", Data: types.ValueMap{"f1": types.String{Val: "v1"}}, Metadata: types.Metadata{Stream: "s", Timestamp: now}},
		{Key: "k2", Data: types.ValueMap{"f2": types.String{Val: "v2"}}, Metadata: types.Metadata{Stream: "s", Timestamp: now}},
	}
	in := make(chan types.Record, len(recs))
	for _, r := range recs {
		in <- r
	}
	close(in)

	svc := &RecordStreamService{InputChan: in}
	out := make(chan *Record, len(recs))
	fs := &fakeStream{ctx: context.Background(), Ch: out}

	err := svc.StreamRecords(&StreamRequest{NodeId: "node"}, fs)
	assert.NoError(t, err)

	close(out)
	var got []types.Record
	for pr := range out {
		r, err := ProtocolBuffersRecordToRecord(pr)
		assert.NoError(t, err)
		got = append(got, r)
	}
	assert.Len(t, got, len(recs))
	for i, exp := range recs {
		act := got[i]
		assert.Equal(t, exp.Key, act.Key)
		assert.Equal(t, exp.Data, act.Data)
		assert.Equal(t, exp.Metadata.Stream, act.Metadata.Stream)
		assert.True(t, exp.Metadata.Timestamp.Equal(act.Metadata.Timestamp))
	}
}

func TestRecordStreamService_StreamRecords_ContextCancelled(t *testing.T) {
	in := make(chan types.Record)
	svc := &RecordStreamService{InputChan: in}
	ch := make(chan *Record)
	ctx, cancel := context.WithCancel(context.Background())
	fs := &fakeStream{ctx: ctx, Ch: ch}

	cancel()
	err := svc.StreamRecords(&StreamRequest{NodeId: "node"}, fs)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestRecordStreamService_StreamRecordsFromLeader_EndToEndSendingRecord(t *testing.T) {
	// start gRPC server
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	srv := grpc.NewServer()
	in := make(chan types.Record)
	RegisterRecordStreamServer(srv, &RecordStreamService{InputChan: in})
	go srv.Serve(lis)
	defer srv.Stop()

	// client
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	recCh, errCh := StreamRecordsFromLeader(ctx, lis.Addr().String(), "node")

	// send a record and then close
	now := time.Now()
	rec := types.Record{Key: "k", Data: types.ValueMap{"f": types.String{Val: "v"}}, Metadata: types.Metadata{Stream: "s", Timestamp: now}}
	in <- rec
	close(in)

	got, ok := <-recCh
	assert.True(t, ok)
	assert.Equal(t, rec.Key, got.Key)
	assert.Equal(t, rec.Data, got.Data)

	// expect error from underlying stream close (e.g., EOF)
	errRecv, ok := <-errCh
	assert.True(t, ok)
	assert.Error(t, errRecv)

	// channels should be closed
	_, ok = <-recCh
	assert.False(t, ok)
	_, ok = <-errCh
	assert.False(t, ok)
}
