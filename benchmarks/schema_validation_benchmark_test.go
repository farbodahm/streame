package benchmarks

import (
	"context"
	"log/slog"
	"testing"

	"github.com/farbodahm/streame/pkg/core"
	. "github.com/farbodahm/streame/pkg/types"
	"github.com/farbodahm/streame/pkg/utils"
)

// heavy_schema_validation_stages creates lots of heavy structs
// to benchmark performance of schema validation
func heavy_schema_validation_stages(number_of_records int) {
	input := make(chan Record)
	output := make(chan Record)
	errors := make(chan error)

	sdf := core.NewStreamDataFrame(input, output, errors, utils.HeavyRecordSchema(),
		core.WithLogLevel(slog.LevelError))

	go func() {
		for i := 0; i < number_of_records; i++ {
			input <- utils.NewHeavyRecord(100)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	go sdf.Execute(ctx)

	for i := 0; i < number_of_records; i++ {
		<-output
	}
	cancel()
}

func BenchmarkSchemaValidation(b *testing.B) {
	for i := 0; i < b.N; i++ {
		heavy_schema_validation_stages(3000)
	}
}
