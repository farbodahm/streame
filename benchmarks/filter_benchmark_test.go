package benchmarks

import (
	"context"
	"testing"

	"github.com/farbodahm/streame/pkg/core"
	"github.com/farbodahm/streame/pkg/functions"
	. "github.com/farbodahm/streame/pkg/types"
	"github.com/farbodahm/streame/pkg/utils"
)

// heavy_filter_stages creates a heavy struct with lots of
// fields and filter stages to take benchmark
func heavy_filter_stages(number_of_stages int, number_of_records int) {
	input := make(chan Record)
	output := make(chan Record)
	errors := make(chan error)

	sdf := core.StreamDataFrame{
		SourceStream: input,
		OutputStream: output,
		ErrorStream:  errors,
		Stages:       []core.Stage{},
	}

	// Create stages
	filter := functions.Filter{
		ColumnName: "Field1",
		Value:      "foobar",
		Operator:   functions.NOT_EQUAL,
	}
	result_df := sdf.Filter(filter)
	for i := 0; i < number_of_stages; i++ {
		result_df = result_df.Filter(filter)
	}

	heavy_record := utils.NewHeavyRecord(20)

	go func() {
		for i := 0; i < number_of_records; i++ {
			input <- heavy_record
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	result_df.Execute(ctx)

	for i := 0; i < number_of_records; i++ {
		<-output
	}
	cancel()
}

func BenchmarkFilterFunction(b *testing.B) {
	for i := 0; i < b.N; i++ {
		heavy_filter_stages(100, 1000)
	}
}
