package benchmarks

import (
	"context"
	"log/slog"
	"testing"

	"github.com/farbodahm/streame/pkg/core"
	. "github.com/farbodahm/streame/pkg/types"
	"github.com/farbodahm/streame/pkg/utils"
)

// heavy_rename_column_stages renames a column
// with lots of records
func heavy_rename_column_stages(number_of_records int) {
	input := make(chan Record)
	output := make(chan Record)
	errors := make(chan error)

	sdf := core.NewStreamDataFrame(input, output, errors, utils.HeavyRecordSchema(), "test-stream",
		core.WithLogLevel(slog.LevelError))

	// Create stages
	result_df := sdf.Rename("field_1", "new_field")

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

func BenchmarkRenameColumnFunction(b *testing.B) {
	for i := 0; i < b.N; i++ {
		heavy_rename_column_stages(3000)
	}
}
