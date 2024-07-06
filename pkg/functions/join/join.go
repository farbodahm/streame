package join

// JoinType specifies how to join 2 data frames
type JoinType int

const (
	Inner JoinType = iota
	Left
)

// JoinCondition specifies conditions that should be met in order for a join to happen
type JoinCondition struct {
	Left  string
	Right string
}
