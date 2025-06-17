package util

import (
	"runtime"

	sf "github.com/tarndt/shardedsingleflight"
)

type SingleFlightGroup = sf.ShardedGroup

func NewSingleFlightGroup() *SingleFlightGroup {
	return sf.NewShardedGroup(sf.WithShardCount(runtime.NumCPU()))
}
