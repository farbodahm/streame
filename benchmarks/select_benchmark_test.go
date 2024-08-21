package benchmarks

import (
	"context"
	"log/slog"
	"testing"

	"github.com/farbodahm/streame/pkg/core"
	. "github.com/farbodahm/streame/pkg/types"
	"github.com/farbodahm/streame/pkg/utils"
)

// heavy_select_stages creates lots of heavy structs
// to benchmark performance of select
func heavy_select_stages(number_of_stages int, number_of_records int) {
	input := make(chan Record)
	output := make(chan Record)
	errors := make(chan error)

	sdf := core.NewStreamDataFrame(input, output, errors, utils.HeavyRecordSchema(), "test-stream",
		core.WithLogLevel(slog.LevelError))

	result_df := sdf.Select("field_1", "field_2", "field_3", "field_4",
		"field_5", "field_6", "field_7", "field_8", "field_9",
		"field_10", "field_11", "field_12", "field_13", "field_14", "field_15")
	for i := 0; i < number_of_stages; i++ {
		result_df = sdf.Select("field_1", "field_2", "field_3", "field_4",
			"field_5", "field_6", "field_7", "field_8", "field_9",
			"field_10", "field_11", "field_12", "field_13", "field_14", "field_15",
		)
	}

	go func() {
		for i := 0; i < number_of_records; i++ {
			input <- utils.NewHeavyRecord(100)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	go result_df.Execute(ctx)

	for i := 0; i < number_of_records; i++ {
		<-output
	}
	cancel()
}

func BenchmarkSelect(b *testing.B) {
	for i := 0; i < b.N; i++ {
		heavy_select_stages(200, 3000)
	}
}
