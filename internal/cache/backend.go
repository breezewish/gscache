package cache

import (
	"context"
	"io"
	"time"

	"github.com/breezewish/gscache/internal/protocol"
)

type PutOpts struct {
	Req  protocol.PutRequest
	Body io.Reader

	// If set, will use this time as the cache entry's time, instead of the current time.
	// This is mainly used when a backend is used in another backend.
	OverrideTime *time.Time

	// Is this Put request part of a compaction process? Used for statistics.
	IsInCompaction bool
}

type GetOpts struct {
	Req protocol.GetRequest

	// Is this Get request part of a compaction process? Used for statistics.
	IsInCompaction bool
}

type Backend interface {
	Put(PutOpts) (*protocol.PutResponse, error)
	Get(GetOpts) (*protocol.GetResponse, error)
	Open(ctx context.Context) error
	Close() error
}

type BackendSupportCompaction interface {
	Backend
	Compact() error
}
