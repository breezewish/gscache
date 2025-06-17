package cacheprog

import (
	"io"

	"github.com/breezewish/gscache/internal/client"
	"github.com/breezewish/gscache/internal/protocol"
)

// CacheHandler abstracts Get and Put so that we can unit test it.
type CacheHandler interface {
	Put(req protocol.PutRequest, body io.Reader) (*protocol.PutResponse, error)
	Get(req protocol.GetRequest) (*protocol.GetResponse, error)
}

// HandlerViaServer simply delegates all cache API calls to the gscache server via a HTTP Client.
type HandlerViaServer struct {
	client *client.Client
}

var _ CacheHandler = (*HandlerViaServer)(nil)

func NewHandlerViaServer(config client.Config) CacheHandler {
	return &HandlerViaServer{
		client: client.NewClient(config),
	}
}

func (c *HandlerViaServer) Put(req protocol.PutRequest, body io.Reader) (*protocol.PutResponse, error) {
	return c.client.CallPut(req, body)
}

func (c *HandlerViaServer) Get(req protocol.GetRequest) (*protocol.GetResponse, error) {
	return c.client.CallGet(req)
}
