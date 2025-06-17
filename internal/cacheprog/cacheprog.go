package cacheprog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/breezewish/gscache/internal/protocol"
	"github.com/breezewish/gscache/internal/util"
)

type CacheProg struct {
	handler CacheHandler

	wg sync.WaitGroup

	lifecycle       context.Context
	lifecycleCancel context.CancelCauseFunc

	// 1 reader, n writers
	reader  *util.LineChunkedReader
	writeMu sync.Mutex // guard jEnc
	jEnc    *json.Encoder
}

type Opts struct {
	CacheHandler CacheHandler
	In           io.Reader
	Out          io.Writer
}

func New(opts Opts) *CacheProg {
	ctx, cancel := context.WithCancelCause(context.Background())

	if opts.In == nil {
		opts.In = os.Stdin
	}
	if opts.Out == nil {
		opts.Out = os.Stdout
	}

	return &CacheProg{
		handler: opts.CacheHandler,

		lifecycle:       ctx,
		lifecycleCancel: cancel,

		reader: util.NewLineChunkedReader(opts.In), // Buf size must be large enough to read a full request
		jEnc:   json.NewEncoder(opts.Out),
	}
}

func (cp *CacheProg) readLoop() error {
	// Any protocol-level errors will cause the Run loop to exit
	// because there is no evidence that following requests can be handled correctly.

	for {
		select {
		case <-cp.lifecycle.Done():
			return cp.lifecycle.Err()
		default:
		}

		line, isPrefix, err := cp.reader.NextValidLine()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("failed to read from stdin: %w", err)
		}
		if isPrefix {
			return fmt.Errorf("unexpected large line from stdin")
		}
		req := protocol.CacheProgRequest{}
		if err := json.Unmarshal(line, &req); err != nil {
			return fmt.Errorf("failed to decode incoming request: %w", err)
		}

		switch req.Command {
		case protocol.CmdClose:
			return nil
		case protocol.CmdPut:
			{
				pipeRead, pipeWrite := io.Pipe()

				cp.runAsync(func() {
					apiResp, err := cp.handler.Put(protocol.PutRequest{
						ActionID: req.ActionID,
						OutputID: req.OutputID,
						BodySize: req.BodySize,
					}, pipeRead)
					if err != nil {
						cp.mustWriteResponse(protocol.CacheProgResponse{
							ID:  req.ID,
							Err: err.Error(),
						})
					} else {
						cp.mustWriteResponse(protocol.CacheProgResponse{
							ID:       req.ID,
							DiskPath: apiResp.DiskPath,
						})
					}
				})

				if req.BodySize == 0 {
					pipeWrite.Close() // No body
				} else {
					for {
						lineChunk, isPrefix, err := cp.reader.NextValidLine()
						if err != nil {
							pipeWrite.CloseWithError(io.ErrClosedPipe)
							return fmt.Errorf("failed to read CmdPut body: %w", err)
						}
						pipeWrite.Write(lineChunk)
						if !isPrefix {
							pipeWrite.Close()
							break
						}
					}
				}
			}
		case protocol.CmdGet:
			cp.runAsync(func() {
				apiResp, err := cp.handler.Get(protocol.GetRequest{
					ActionID: req.ActionID,
				})
				if err != nil {
					cp.mustWriteResponse(protocol.CacheProgResponse{
						ID:  req.ID,
						Err: err.Error(),
					})
				} else {
					cp.mustWriteResponse(protocol.CacheProgResponse{
						ID:       req.ID,
						Miss:     apiResp.Miss,
						OutputID: apiResp.OutputID,
						Size:     apiResp.Size,
						Time:     apiResp.Time,
						DiskPath: apiResp.DiskPath,
					})
				}
			})
		default:
			cp.runAsync(func() {
				cp.mustWriteResponse(protocol.CacheProgResponse{
					ID:  req.ID,
					Err: fmt.Sprintf("unknown command %s", req.Command),
				})
			})
		}
	}
}

func (cp *CacheProg) runAsync(fn func()) {
	cp.wg.Add(1)
	go func() {
		defer cp.wg.Done()
		fn()
	}()
}

// Run starts the CacheProg and handles incoming requests via stdin / stdout
// until a close command is received (returns nil) or an error occurs.
func (cp *CacheProg) Run() error {
	err := cp.sendInitialCapability()
	if err != nil {
		return fmt.Errorf("failed to send initial capability: %w", err)
	}

	defer cp.wg.Wait()

	// cp.readLoop actually blocks when reading. It does not stop in time when the lifecycle is cancelled.
	// So here we wait for lifecycle cancellation directly.

	go func() {
		err := cp.readLoop()
		cp.lifecycleCancel(err)
	}()
	<-cp.lifecycle.Done()

	err = context.Cause(cp.lifecycle)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func (cp *CacheProg) writeResponse(resp protocol.CacheProgResponse) error {
	cp.writeMu.Lock()
	defer cp.writeMu.Unlock()

	// Note: json encoder always write a \n at the end of each call
	if err := cp.jEnc.Encode(resp); err != nil {
		// Possibly marshal error or write pipe error
		// TODO: If it is a pipe error, we should handle it gracefully
		errResp := protocol.CacheProgResponse{
			ID:  resp.ID,
			Err: fmt.Sprintf("failed to encode response: %s", err),
		}
		err = cp.jEnc.Encode(errResp)
		if err != nil {
			return fmt.Errorf("failed to write error response: %w", err)
		}
	}

	return nil
}

func (cp *CacheProg) mustWriteResponse(resp protocol.CacheProgResponse) {
	if err := cp.writeResponse(resp); err != nil {
		cp.lifecycleCancel(err)
	}
}

func (cp *CacheProg) sendInitialCapability() error {
	return cp.writeResponse(protocol.CacheProgResponse{
		ID: 0,
		KnownCommands: []protocol.Cmd{
			protocol.CmdPut,
			protocol.CmdGet,
			protocol.CmdClose,
		},
	})
}
