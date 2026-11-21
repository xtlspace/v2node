package panel

import (
	"context"
	"errors"
	"fmt"
	stdnet "net"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/go-resty/resty/v2"
	"github.com/wyx2685/v2node/conf"
)

// Panel is the interface for different panel's api.

type Client struct {
	client           *resty.Client
	router           *transportRouter
	APIHost          string
	Token            string
	NodeId           int
	nodeEtag         string
	userEtag         string
	responseBodyHash string
	UserList         *UserListBody
	AliveMap         *AliveMap
}

func New(c *conf.NodeConfig) (*Client, error) {
	client := resty.New()
	retryCount := conf.DefaultNodeRetryCount
	if c.RetryCount != nil {
		retryCount = *c.RetryCount
	}
	client.SetRetryCount(retryCount)
	client.SetHeader("User-Agent", fmt.Sprintf("v2node go-resty/%s (https://github.com/go-resty/resty)", resty.Version))
	if c.Timeout > 0 {
		client.SetTimeout(time.Duration(c.Timeout) * time.Second)
	} else {
		client.SetTimeout(time.Duration(conf.DefaultNodeTimeout) * time.Second)
	}
	client.OnError(func(req *resty.Request, err error) {
		var v *resty.ResponseError
		if errors.As(err, &v) {
			// v.Response contains the last response from the server
			// v.Err contains the original error
			logrus.Error(v.Err)
		}
	})
	client.SetBaseURL(c.APIHost)
	// set params
	client.SetQueryParams(map[string]string{
		"node_type": "v2node",
		"node_id":   strconv.Itoa(c.NodeID),
		"token":     c.Key,
	})
	// Route requests by method: GET stays direct (used to bootstrap before the
	// core exists), POST can be dialed through the xray dispatcher once the
	// core is up (installed by SetPostDialer).
	router := &transportRouter{direct: http.DefaultTransport}
	client.SetTransport(router)
	return &Client{
		client:   client,
		router:   router,
		Token:    c.Key,
		APIHost:  c.APIHost,
		NodeId:   c.NodeID,
		UserList: &UserListBody{},
		AliveMap: &AliveMap{},
	}, nil
}

// transportRouter sends GET (and any non-POST) requests through the direct
// transport, and POST requests through the xray dispatcher transport once it
// has been installed.
type transportRouter struct {
	direct http.RoundTripper
	post   atomic.Pointer[http.Transport]
}

func (t *transportRouter) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method == http.MethodPost {
		if tr := t.post.Load(); tr != nil {
			return tr.RoundTrip(req)
		}
	}
	return t.direct.RoundTrip(req)
}

// SetPostDialer installs the dialer used for POST requests (the xray
// dispatcher). Passing nil is a no-op; until it is set, POST falls back to the
// direct transport.
func (c *Client) SetPostDialer(dial func(ctx context.Context, network, addr string) (stdnet.Conn, error)) {
	if dial == nil {
		return
	}
	c.router.post.Store(&http.Transport{
		DialContext:       dial,
		ForceAttemptHTTP2: false,
		MaxIdleConns:      8,
		IdleConnTimeout:   60 * time.Second,
	})
}
