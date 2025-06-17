package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/breezewish/gscache/internal/protocol"
	"github.com/go-resty/resty/v2"
)

type Config struct {
	DaemonPort int
}

// Client talks to a gscache server daemon via HTTP REST API
type Client struct {
	client *resty.Client
	config Config
}

func NewClient(config Config) *Client {
	client := resty.New().
		SetTimeout(30 * time.Second).
		SetBaseURL(fmt.Sprintf("http://127.0.0.1:%d", config.DaemonPort)).
		SetError(&protocol.ErrorResponse{})
	return &Client{
		client: client,
		config: config,
	}
}

type ClientError struct {
	msg string
}

func (e ClientError) Error() string {
	return e.msg
}

func newClientError(resp *resty.Response) error {
	return ClientError{
		msg: resp.Error().(*protocol.ErrorResponse).Error,
	}
}

func (c *Client) CallShutdown() (*protocol.ShutdownResponse, error) {
	r, err := c.client.R().
		SetResult(&protocol.ShutdownResponse{}).
		Post("/shutdown")
	if err != nil {
		return nil, err
	}
	if r.IsError() {
		return nil, newClientError(r)
	}
	return r.Result().(*protocol.ShutdownResponse), nil
}

func (c *Client) CallStatsClear() (*protocol.StatsClearResponse, error) {
	r, err := c.client.R().
		SetResult(&protocol.StatsClearResponse{}).
		Post("/stats/clear")
	if err != nil {
		return nil, err
	}
	if r.IsError() {
		return nil, newClientError(r)
	}
	return r.Result().(*protocol.StatsClearResponse), nil
}

func (c *Client) CallPing() (*protocol.PingResponse, error) {
	r, err := c.client.R().
		SetResult(&protocol.PingResponse{}).
		Get("/ping")
	if err != nil {
		return nil, err
	}
	if r.IsError() {
		return nil, newClientError(r)
	}
	return r.Result().(*protocol.PingResponse), nil
}

func (c *Client) CallPut(req protocol.PutRequest, encodedPayload io.Reader) (*protocol.PutResponse, error) {
	// Note: Unlike other APIs, PUT is carefully designed in a streaming way

	encodedReq := bytes.NewBuffer(nil)
	enc := json.NewEncoder(encodedReq)
	err := enc.Encode(req)
	if err != nil {
		return nil, err
	}

	var bodyReader io.Reader
	if req.BodySize > 0 {
		bodyReader = io.MultiReader(encodedReq, encodedPayload)
	} else {
		bodyReader = encodedReq
	}

	r, err := c.client.R().
		SetResult(&protocol.PutResponse{}).
		SetBody(bodyReader).
		SetHeader("Content-Type", "application/octet-stream").
		Post("/cacheprog/put")
	if err != nil {
		return nil, err
	}
	if r.IsError() {
		return nil, newClientError(r)
	}
	resp := r.Result().(*protocol.PutResponse)
	return resp, nil
}

func (c *Client) CallGet(req protocol.GetRequest) (*protocol.GetResponse, error) {
	r, err := c.client.R().
		SetResult(&protocol.GetResponse{}).
		SetBody(req).
		Post("/cacheprog/get")
	if err != nil {
		return nil, err
	}
	if r.IsError() {
		return nil, newClientError(r)
	}
	resp := r.Result().(*protocol.GetResponse)
	return resp, nil
}
