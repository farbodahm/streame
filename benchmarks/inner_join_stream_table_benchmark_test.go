package benchmarks

import (
	"context"
	"log/slog"
	"sync"
	"testing"

	"github.com/farbodahm/streame/pkg/core"
	"github.com/farbodahm/streame/pkg/functions/join"
	. "github.com/farbodahm/streame/pkg/types"
	"github.com/farbodahm/streame/pkg/utils"
	"golang.org/x/exp/rand"
)

func inner_join_stream_table(number_of_records int) {
	// Stream1
	stream1_input := make(chan Record)
	stream1_output := make(chan Record)
	stream1_errors := make(chan error)
	stream1_sdf := core.NewStreamDataFrame(stream1_input, stream1_output, stream1_errors, utils.HeavyRecordSchema(), "benchmark1", core.WithLogLevel(slog.LevelError))

	// Stream2
	stream2_input := make(chan Record)
	stream2_output := make(chan Record)
	stream2_errors := make(chan error)
	stream2_sdf := core.NewStreamDataFrame(stream2_input, stream2_output, stream2_errors, utils.HeavyRecordSchemaV2(), "benchmark2 ", core.WithLogLevel(slog.LevelError))

	// Logic to test
	joined_sdf := stream2_sdf.Join(&stream1_sdf, join.Inner, join.JoinCondition{LeftKey: "field_21", RightKey: "field_1"}, join.StreamTable).(*core.StreamDataFrame)

	// shared_keys is used to make sure all stream records will match to a table record
	shared_keys := []string{}
	// wg is used to make sure table is populated first
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		for i := 0; i < number_of_records; i++ {
			heavy_record := utils.NewHeavyRecord(100)
			shared_keys = append(shared_keys, heavy_record.Data["field_1"].ToString())
			stream1_input <- heavy_record
		}
	}()

	go func() {
		wg.Wait()
		for i := 0; i < number_of_records; i++ {
			heavy_record_v2 := utils.NewHeavyRecordV2(100)
			heavy_record_v2.Data["field_21"] = String{Val: shared_keys[rand.Intn(len(shared_keys))]}
			stream2_input <- heavy_record_v2
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	go joined_sdf.Execute(ctx)

	for i := 0; i < number_of_records; i++ {
		<-joined_sdf.OutputStream
	}
	cancel()

}

func BenchmarkStreamTableInnerJoin(b *testing.B) {
	for i := 0; i < b.N; i++ {
		inner_join_stream_table(1000)
	}
}
