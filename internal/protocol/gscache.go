package protocol

import (
	"fmt"
	"time"

	"go.uber.org/zap/zapcore"
)

// These protocols are used for communication between the gscache server and client.

type PingResponse struct {
	Status string
	Pid    int
	Config any
}

type ShutdownResponse struct {
}

type StatsClearResponse struct {
}

type ErrorResponse struct {
	Error string
}

type GetRequest struct {
	ActionID []byte `json:",omitempty"` // or nil if not used
}

func (r *GetRequest) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	if r == nil {
		enc.AddReflected(".", nil)
		return nil
	}
	enc.AddString("actionID", fmt.Sprintf("%x", r.ActionID))
	return nil
}

type GetResponse struct {
	Miss     bool       `json:",omitempty"` // cache miss
	OutputID []byte     `json:",omitempty"` // the OutputID stored with the body
	Size     int64      `json:",omitempty"` // body size in bytes
	Time     *time.Time `json:",omitempty"` // when the object was put in the cache (optional; used for cache expiration)
	// DiskPath is the absolute path on disk of the body corresponding to a
	// "get" (on cache hit) or "put" request's ActionID.
	DiskPath string `json:",omitempty"`
}

func (r *GetResponse) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	if r == nil {
		enc.AddReflected(".", nil)
		return nil
	}
	if !r.Miss {
		enc.AddString("outputID", fmt.Sprintf("%x", r.OutputID))
		enc.AddString("diskPath", r.DiskPath)
	} else {
		enc.AddBool("miss", r.Miss)
	}
	return nil
}

type PutRequest struct {
	ActionID []byte `json:",omitempty"` // or nil if not used
	OutputID []byte `json:",omitempty"` // or nil if not used
	// BodySize is the number of bytes of Body. If zero, the body isn't written.
	BodySize int64 `json:",omitempty"`
}

func (r *PutRequest) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	if r == nil {
		enc.AddReflected(".", nil)
		return nil
	}
	enc.AddString("actionID", fmt.Sprintf("%x", r.ActionID))
	enc.AddString("outputID", fmt.Sprintf("%x", r.OutputID))
	enc.AddInt64("size", r.BodySize)
	return nil
}

type PutResponse struct {
	// DiskPath is the absolute path on disk of the body corresponding to a
	// "get" (on cache hit) or "put" request's ActionID.
	DiskPath string `json:",omitempty"`
}

func (r *PutResponse) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	if r == nil {
		enc.AddReflected(".", nil)
		return nil
	}
	enc.AddString("diskPath", r.DiskPath)
	return nil
}
