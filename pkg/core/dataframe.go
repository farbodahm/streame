package core

import (
	"context"

	"github.com/farbodahm/streame/pkg/functions"
	"github.com/farbodahm/streame/pkg/types"
)

// DataFrame is a collection of rows and columns
type DataFrame interface {
	Filter(filter functions.Filter) DataFrame
	Select(columns ...string) DataFrame
	AddStaticColumn(name string, value types.ColumnValue) DataFrame
	Rename(old_name string, new_name string) DataFrame
	Execute(ctx context.Context) error
	GetSchema() types.Schema
}
