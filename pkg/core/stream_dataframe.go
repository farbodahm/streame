package core

import "github.com/farbodahm/streame/pkg/types"

// StreamDataFrame represents an un-bounded DataFrame
type StreamDataFrame struct {
	SourceStream chan (types.DataRecord)
	OutputStream chan (types.DataRecord)
	Stages       []Stage
}

// func (sdf StreamDataFrame) Run() {
// 	for {
// 		select {
// 		case record := <-sdf.SourceStream:
// 			for _, stage := range sdf.Stages {
// 				record = stage.Process(record)
// 			}
// 			sdf.OutputStream <- record
// 		}
// 	}
// }
