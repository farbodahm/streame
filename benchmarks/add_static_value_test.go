package benchmarks

import (
	"context"
	"fmt"
	"log/slog"
	"testing"

	"github.com/farbodahm/streame/pkg/core"
	. "github.com/farbodahm/streame/pkg/types"
	"github.com/farbodahm/streame/pkg/utils"
)

// heavy_static_column_stages creates lots of stages for adding
// a static column to the dataframe
func heavy_static_column_stages(number_of_stages int, number_of_records int) {
	input := make(chan Record)
	output := make(chan Record)
	errors := make(chan error)

	sdf := core.NewStreamDataFrame(input, output, errors, utils.HeavyRecordSchema(),
		core.WithLogLevel(slog.LevelError))

	// Create stages
	result_df := sdf.AddStaticColumn("column_0", String{Val: "static_value"})
	for i := 1; i < number_of_stages; i++ {
		result_df = result_df.AddStaticColumn(
			fmt.Sprintf("column_%d", i+1),
			String{Val: "static_value"},
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

func BenchmarkAddStaticColumnFunction(b *testing.B) {
	for i := 0; i < b.N; i++ {
		heavy_static_column_stages(200, 3000)
	}
}
