package server

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/breezewish/gscache/internal/cache"
	"github.com/breezewish/gscache/internal/log"
	"github.com/breezewish/gscache/internal/protocol"
	"github.com/breezewish/gscache/internal/stats"
	"github.com/caarlos0/httperr"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

func (s *Server) newRouter() *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())
	router.Use(mCatchError)

	router.GET("/ping", s.handlePing)
	router.POST("/shutdown", s.handleShutdown)
	router.POST("/stats/clear", s.handleStatsClear)
	router.POST("/cacheprog/put", s.mMarkActive, s.handleCachePut)
	router.POST("/cacheprog/get", s.mMarkActive, s.handleCacheGet)

	return router
}

// mMarkActive is a middleware marks this server as recently active.
func (s *Server) mMarkActive(c *gin.Context) {
	select {
	case s.activityCh <- struct{}{}:
	default:
	}
	c.Next()
}

// mCatchError is a middleware turns errors into a standard JSON error response.
func mCatchError(c *gin.Context) {
	c.Next()
	if len(c.Errors) > 0 {
		err := c.Errors.Last().Err
		log.Error("Request failed",
			zap.String("remoteAddr", c.Request.RemoteAddr),
			zap.String("path", c.Request.URL.Path),
			zap.Error(err))
		if httperr, ok := err.(*httperr.Error); ok {
			c.JSON(httperr.Status, protocol.ErrorResponse{Error: httperr.Error()})
		} else {
			c.JSON(http.StatusInternalServerError, protocol.ErrorResponse{Error: err.Error()})
		}
	}
}

// GET /ping
func (s *Server) handlePing(c *gin.Context) {
	log.Debug("/ping", zap.String("remoteAddr", c.Request.RemoteAddr))
	c.JSON(http.StatusOK, protocol.PingResponse{
		Status: "ok",
		Pid:    os.Getpid(),
		Config: s.config, // TODO: Remove sensitive data
	})
}

// POST /shutdown
func (s *Server) handleShutdown(c *gin.Context) {
	log.Info("/shutdown", zap.String("remoteAddr", c.Request.RemoteAddr))
	c.JSON(http.StatusOK, protocol.ShutdownResponse{})
	s.Shutdown()
}

// POST /stats/clear
func (s *Server) handleStatsClear(c *gin.Context) {
	log.Info("/stats/clear", zap.String("remoteAddr", c.Request.RemoteAddr))
	stats.Default.Clear()
	stats.Default.ForcePersist()
	c.JSON(http.StatusOK, protocol.StatsClearResponse{})
}

// quoteCloseReader emits EOF when meets a quote and swallows the quote.
// It is used to streamingly read the cache body with a Base64 decoder
// which is like:
// "<BASE64_ENCODED_DATA>"
type quoteCloseReader struct {
	wrapped io.Reader
	closed  bool
}

func (r *quoteCloseReader) Read(p []byte) (int, error) {
	if r.closed {
		return 0, io.EOF
	}
	n, err := r.wrapped.Read(p)
	if n > 0 {
		for i := 0; i < n; i++ {
			if p[i] == '"' {
				r.closed = true
				return i, err
			}
		}
	}
	return n, err
}

func decodePut(r io.Reader) (*protocol.PutRequest, io.Reader, error) {
	reader := bufio.NewReader(r)
	jsonLine, err := reader.ReadBytes('\n')
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read Put request: %v", err)
	}
	var putReq protocol.PutRequest
	if err := json.Unmarshal(jsonLine, &putReq); err != nil {
		return nil, nil, fmt.Errorf("failed to parse Put request: %v", err)
	}

	if putReq.BodySize == 0 {
		return &putReq, bytes.NewReader(nil), nil
	}

	// First byte must be a quote (").
	firstByte, err := reader.ReadByte()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read Put body: %v", err)
	}
	if firstByte != '"' {
		return nil, nil, fmt.Errorf("unexpected Put body first byte: %q", firstByte)
	}
	// Last byte must be a quote (").
	restReader := base64.NewDecoder(base64.StdEncoding, &quoteCloseReader{wrapped: reader})
	return &putReq, restReader, nil
}

// POST /cacheprog/put
func (s *Server) handleCachePut(c *gin.Context) {
	defer c.Request.Body.Close()
	req, putPayloadReader, err := decodePut(c.Request.Body)
	if err != nil {
		c.Error(httperr.Wrap(err, http.StatusBadRequest))
		return
	}

	defer stats.Default.Persist()
	stats.Default.PutTotal.Inc()

	resp, err := s.backend.Put(cache.PutOpts{
		Req:  *req,
		Body: putPayloadReader,
	})
	if err != nil {
		stats.Default.PutError.Inc()
		c.Error(err)
		return
	}

	log.Debug("/cacheprog/get", zap.Object("request", req), zap.Object("response", resp))
	c.JSON(http.StatusOK, resp)
}

// POST /cacheprog/get
func (s *Server) handleCacheGet(c *gin.Context) {
	var req protocol.GetRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Error(httperr.Errorf(http.StatusBadRequest, "failed to read Get request: %v", err))
		return
	}

	defer stats.Default.Persist()
	stats.Default.GetTotal.Inc()

	resp, err := s.backend.Get(cache.GetOpts{
		Req: req,
	})
	if err != nil {
		stats.Default.GetError.Inc()
		c.Error(err)
		return
	}
	if resp.Miss {
		stats.Default.GetMiss.Inc()
	} else {
		stats.Default.GetHit.Inc()
	}

	log.Debug("/cacheprog/get", zap.Object("request", &req), zap.Object("response", resp))
	c.JSON(http.StatusOK, resp)
}
