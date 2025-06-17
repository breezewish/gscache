package client

import (
	"context"
	"errors"
	"fmt"
	"syscall"
	"time"

	"github.com/breezewish/gscache/internal/protocol"
)

func (c *Client) IsDaemonAlive() (bool, error) {
	_, err := c.CallPing()
	if err != nil {
		if errors.Is(err, syscall.ECONNREFUSED) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (c *Client) WaitServerAlive(ctx context.Context, maxWait time.Duration) (*protocol.PingResponse, error) {
	t := time.Now()
	for {
		if ctx.Err() != nil {
			return nil, context.Cause(ctx)
		}
		resp, err := c.CallPing()
		if err == nil {
			return resp, nil
		}
		if time.Since(t) > maxWait {
			return nil, fmt.Errorf("timed out after %s", maxWait)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (c *Client) ShutdownAndWait(maxWait time.Duration) (bool, error) {
	_, err := c.CallShutdown()
	if err != nil {
		if errors.Is(err, syscall.ECONNREFUSED) {
			return false, nil
		}
		return false, err
	}

	t := time.Now()
	for {
		alive, _ := c.IsDaemonAlive()
		if !alive {
			break
		}
		time.Sleep(100 * time.Millisecond)
		if time.Since(t) > maxWait {
			return false, fmt.Errorf("timed out after %s", maxWait)
		}
	}

	return true, nil
}
