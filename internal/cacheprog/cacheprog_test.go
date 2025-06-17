package cacheprog

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/breezewish/gscache/internal/protocol"
)

type mockHandler struct {
	putCalls []putCall
	getCalls []getCall
	putError error
	getError error
	putResp  *protocol.PutResponse
	getResp  *protocol.GetResponse
}

var _ CacheHandler = (*mockHandler)(nil)

type putCall struct {
	req         protocol.PutRequest
	encodedBody []byte
}

type getCall struct {
	req protocol.GetRequest
}

func (m *mockHandler) Put(req protocol.PutRequest, body io.Reader) (*protocol.PutResponse, error) {
	bodyBytes, _ := io.ReadAll(body)
	m.putCalls = append(m.putCalls, putCall{req: req, encodedBody: bodyBytes})
	if m.putError != nil {
		return nil, m.putError
	}
	if m.putResp != nil {
		return m.putResp, nil
	}
	return &protocol.PutResponse{DiskPath: "/tmp/test"}, nil
}

func (m *mockHandler) Get(req protocol.GetRequest) (*protocol.GetResponse, error) {
	m.getCalls = append(m.getCalls, getCall{req: req})
	if m.getError != nil {
		return nil, m.getError
	}
	if m.getResp != nil {
		return m.getResp, nil
	}
	return &protocol.GetResponse{Miss: false, OutputID: []byte("output"), Size: 100, DiskPath: "/tmp/test"}, nil
}

func TestCacheProg_InitialCapability(t *testing.T) {
	handler := &mockHandler{}
	var output bytes.Buffer

	cp := New(Opts{
		CacheHandler: handler,
		In:           strings.NewReader(`{"ID":1,"Command":"close"}`),
		Out:          &output,
	})

	err := cp.Run()
	require.NoError(t, err)

	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	require.Len(t, lines, 1)

	require.JSONEq(t, `{"ID": 0,"KnownCommands":["put", "get", "close"]}`, lines[0])
}

func TestCacheProg_SendInitialCapabilityFailed(t *testing.T) {
	handler := &mockHandler{}

	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	defer inW.Close()
	defer outR.Close()

	outR.CloseWithError(errors.New("write error"))

	cp := New(Opts{
		CacheHandler: handler,
		In:           inR,
		Out:          outW,
	})

	err := cp.Run()
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to send initial capability")
}

func TestCacheProg_MustWriteResponseError(t *testing.T) {
	handler := &mockHandler{}

	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	defer inW.Close()
	defer outR.Close()

	outRLineReader := bufio.NewReader(outR)

	cp := New(Opts{
		CacheHandler: handler,
		In:           inR,
		Out:          outW,
	})

	errCh := make(chan error, 1)
	go func() {
		errCh <- cp.Run()
	}()

	handshake, isPrefix, err := outRLineReader.ReadLine()
	require.NoError(t, err)
	require.False(t, isPrefix)
	require.JSONEq(t, `{"ID":0,"KnownCommands":["put","get","close"]}`, string(handshake))

	outR.CloseWithError(errors.New("write error"))

	inW.Write([]byte(`{"ID":1,"Command":"get","ActionID":"test"}` + "\n"))

	err = <-errCh
	require.Error(t, err)
	require.Contains(t, err.Error(), "write error")
}

func TestCacheProg_BadMessage(t *testing.T) {
	handler := &mockHandler{}

	cp := New(Opts{
		CacheHandler: handler,
		In:           strings.NewReader(`{"invalid json`),
		Out:          &bytes.Buffer{},
	})

	err := cp.Run()
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to decode incoming request")
}

func TestCacheProg_Get(t *testing.T) {
	handler := &mockHandler{}
	var output bytes.Buffer

	// base64(test-action-id) = dGVzdC1hY3Rpb24taWQ=
	// base64(output) = b3V0cHV0

	cp := New(Opts{
		CacheHandler: handler,
		In: strings.NewReader(`
{"ID":1,"Command":"get","ActionID":"dGVzdC1hY3Rpb24taWQ="}
{"ID":2,"Command":"close"}
`),
		Out: &output,
	})

	err := cp.Run()
	require.NoError(t, err)

	require.Len(t, handler.getCalls, 1)
	require.Equal(t, []byte("test-action-id"), handler.getCalls[0].req.ActionID)

	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	require.Len(t, lines, 2)
	require.JSONEq(t, `{
		"ID": 1,
		"OutputID": "b3V0cHV0",
		"Size": 100,
		"DiskPath": "/tmp/test"
	}`, lines[1])
}

func TestCacheProg_Put(t *testing.T) {
	handler := &mockHandler{}
	var output bytes.Buffer

	// base64(test-action-id) = dGVzdC1hY3Rpb24taWQ=
	// base64(test-output-id) = dGVzdC1vdXRwdXQtaWQ=
	// base64(output) = b3V0cHV0

	cp := New(Opts{
		CacheHandler: handler,
		In: strings.NewReader(`
{"ID":1,"Command":"put","ActionID":"dGVzdC1hY3Rpb24taWQ=","OutputID":"dGVzdC1vdXRwdXQtaWQ=","BodySize":9}
"dGVzdC1ib2R5"
{"ID":2,"Command":"close"}
`),
		Out: &output,
	})

	err := cp.Run()
	require.NoError(t, err)

	require.Len(t, handler.putCalls, 1)
	require.Equal(t, []byte("test-action-id"), handler.putCalls[0].req.ActionID)
	require.Equal(t, []byte("test-output-id"), handler.putCalls[0].req.OutputID)
	require.Equal(t, int64(9), handler.putCalls[0].req.BodySize)
	require.Equal(t, []byte(`"dGVzdC1ib2R5"`), handler.putCalls[0].encodedBody)

	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	require.Len(t, lines, 2)

	require.JSONEq(t, `{
		"ID": 1,
		"DiskPath": "/tmp/test"
	}`, lines[1])
}

func TestCacheProg_PutNoBody(t *testing.T) {
	// Test put message with no body (BodySize = 0)
	handler := &mockHandler{}
	var output bytes.Buffer

	cp := New(Opts{
		CacheHandler: handler,
		In: strings.NewReader(`
{"ID":1,"Command":"put","ActionID":"dGVzdC1hY3Rpb24taWQ=","OutputID":"dGVzdC1vdXRwdXQtaWQ=","BodySize":0}
{"ID":2,"Command":"close"}
`),
		Out: &output,
	})

	err := cp.Run()
	require.NoError(t, err)

	require.Len(t, handler.putCalls, 1)
	require.Equal(t, []byte("test-action-id"), handler.putCalls[0].req.ActionID)
	require.Equal(t, int64(0), handler.putCalls[0].req.BodySize)
	require.Empty(t, handler.putCalls[0].encodedBody)
}

func TestCacheProg_MultipleMessages(t *testing.T) {
	handler := &mockHandler{}
	var output bytes.Buffer

	// base64(test-action-1) = dGVzdC1hY3Rpb24tMQ==
	// base64(test-action-2) = dGVzdC1hY3Rpb24tMg==

	cp := New(Opts{
		CacheHandler: handler,
		In: strings.NewReader(`

{"ID":1,"Command":"get","ActionID":"dGVzdC1hY3Rpb24tMQ=="}


{"ID":2,"Command":"get","ActionID":"dGVzdC1hY3Rpb24tMg=="}

{"ID":3,"Command":"close"}


`),
		Out: &output,
	})

	err := cp.Run()
	require.NoError(t, err)

	require.Len(t, handler.getCalls, 2)
	// Don't assume order since operations are async
	actionIDs := make([]string, 2)
	for i, call := range handler.getCalls {
		actionIDs[i] = string(call.req.ActionID)
	}
	require.Contains(t, actionIDs, "test-action-1")
	require.Contains(t, actionIDs, "test-action-2")

	// Verify responses were sent
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	require.Len(t, lines, 3)

	require.JSONEq(t, `{"ID":0,"KnownCommands":["put","get","close"]}`, lines[0])
	// Responses can come in any order due to async, so just verify the content
	for _, line := range lines[1:] {
		require.Contains(t, []string{
			`{"ID":1,"OutputID":"b3V0cHV0","Size":100,"DiskPath":"/tmp/test"}`,
			`{"ID":2,"OutputID":"b3V0cHV0","Size":100,"DiskPath":"/tmp/test"}`,
		}, line)
	}
}

func TestCacheProg_GetHandlerError(t *testing.T) {
	handler := &mockHandler{
		getError: errors.New("get handler error"),
	}
	var output bytes.Buffer

	cp := New(Opts{
		CacheHandler: handler,
		In: strings.NewReader(`
{"ID":1,"Command":"get","ActionID":"dGVzdC1hY3Rpb24taWQ="}
{"ID":2,"Command":"close"}
`),
		Out: &output,
	})

	err := cp.Run()
	require.NoError(t, err)

	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	require.Len(t, lines, 2)
	require.JSONEq(t, `{"ID":1,"Err":"get handler error"}`, lines[1])
}

func TestCacheProg_PutHandlerError(t *testing.T) {
	handler := &mockHandler{
		putError: errors.New("put handler error"),
	}
	var output bytes.Buffer

	cp := New(Opts{
		CacheHandler: handler,
		In: strings.NewReader(`
{"ID":1,"Command":"put","ActionID":"dGVzdC1hY3Rpb24taWQ=","OutputID":"dGVzdC1vdXRwdXQtaWQ=","BodySize":4}
test
{"ID":2,"Command":"close"}
`),
		Out: &output,
	})

	err := cp.Run()
	require.NoError(t, err)

	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	require.Len(t, lines, 2)
	require.JSONEq(t, `{"ID":1,"Err":"put handler error"}`, lines[1])
}

func TestCacheProg_UnknownCommand(t *testing.T) {
	handler := &mockHandler{}
	var output bytes.Buffer

	cp := New(Opts{
		CacheHandler: handler,
		In: strings.NewReader(`
{"ID":1,"Command":"xx"}
{"ID":2,"Command":"close"}
`),
		Out: &output,
	})

	err := cp.Run()
	require.NoError(t, err)

	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	require.Len(t, lines, 2)
	require.JSONEq(t, `{"ID":1,"Err":"unknown command xx"}`, lines[1])
}

func TestCacheProg_LargeLine(t *testing.T) {
	handler := &mockHandler{}
	var output bytes.Buffer

	largeData := strings.Repeat("a", 5000)
	largeReq := protocol.CacheProgRequest{
		ID:       1,
		Command:  protocol.CmdGet,
		ActionID: []byte(largeData),
	}
	reqJson, _ := json.Marshal(largeReq)

	cp := New(Opts{
		CacheHandler: handler,
		In:           strings.NewReader(string(reqJson)),
		Out:          &output,
	})

	err := cp.Run()
	require.Error(t, err)
	require.Contains(t, err.Error(), "unexpected large line from stdin")
}

func TestCacheProg_LargePut(t *testing.T) {
	handler := &mockHandler{}
	var output bytes.Buffer

	// Create large payload > 4096 bytes
	largeBody := `"` + strings.Repeat("a", 7000) + `"`

	cp := New(Opts{
		CacheHandler: handler,
		In: strings.NewReader(fmt.Sprintf(`
{"ID":1,"Command":"put","ActionID":"dGVzdC1hY3Rpb24taWQ=","OutputID":"dGVzdC1vdXRwdXQtaWQ=","BodySize":123}
%s
{"ID":2,"Command":"close"}
`, largeBody)),
		Out: &output,
	})

	err := cp.Run()
	require.NoError(t, err)

	require.Len(t, handler.putCalls, 1)
	require.Equal(t, []byte("test-action-id"), handler.putCalls[0].req.ActionID)
	require.Equal(t, []byte("test-output-id"), handler.putCalls[0].req.OutputID)
	require.Equal(t, int64(123), handler.putCalls[0].req.BodySize)
	require.Equal(t, []byte(largeBody), handler.putCalls[0].encodedBody)

	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	require.Len(t, lines, 2)
	require.JSONEq(t, `{"ID":1,"DiskPath":"/tmp/test"}`, lines[1])
}
