package core

import (
	"context"

	"github.com/farbodahm/streame/pkg/types"
)

// Stage represents a processing step.
// Input of each stage, is the output of the previous stage.
type Stage struct {
	Id     string
	Input  chan (types.Record)
	Output chan (types.Record)
	Error  chan error

	Executor func(ctx context.Context, data types.Record) ([]types.Record, error)
}

// Run runs the stage.
// It reads data from the input channel, processes it using the Executor function, and sends the processed data to the output channel.
// It exits the loop if the context is cancelled.
// It sends any errors encountered during processing to the error channel.
func (s *Stage) Run(ctx context.Context) {
	for {
		select {
		case data := <-s.Input:
			processedData, err := s.Executor(ctx, data)
			if err != nil {
				s.Error <- err
				continue
			}

			for _, p := range processedData {
				s.Output <- p
			}
		case <-ctx.Done():
			return // Exit the loop if the context is cancelled
		}
	}
}
