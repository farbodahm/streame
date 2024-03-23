package core

import "github.com/farbodahm/streame/pkg/functions"

// DataFrame is a collection of rows and columns
type DataFrame interface {
	Filter(filter functions.Filter) (DataFrame, error)
}
