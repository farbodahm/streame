package join

// JoinType specifies how to join 2 data frames
type JoinType int

const (
	Inner JoinType = iota
	Left
)

// JoinMode specifies the way to see input streams
type JoinMode int

const (
	StreamTable JoinMode = iota
	StreamStream
)

// JoinCondition specifies conditions that should be met in order for a join to happen
type JoinCondition struct {
	LeftKey  string
	RightKey string
}
