package panel

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/wyx2685/v2node/conf"
)

func newAliveTestClient(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	c, err := New(&conf.NodeConfig{APIHost: srv.URL, NodeID: 1, Key: "test"})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return c
}

func TestGetUserAliveUnsupported(t *testing.T) {
	c := newAliveTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})

	alive, err := c.GetUserAlive(context.Background())
	if !errors.Is(err, ErrAliveListUnsupported) {
		t.Fatalf("GetUserAlive() error = %v, want ErrAliveListUnsupported", err)
	}
	if alive != nil {
		t.Fatalf("GetUserAlive() alive = %v, want nil", alive)
	}
}

func TestGetUserAliveServerError(t *testing.T) {
	c := newAliveTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})

	if _, err := c.GetUserAlive(context.Background()); err == nil || errors.Is(err, ErrAliveListUnsupported) {
		t.Fatalf("GetUserAlive() error = %v, want plain error", err)
	}
}

func TestGetUserAliveSuccess(t *testing.T) {
	c := newAliveTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"alive":{"1":2,"3":4}}`))
	})

	alive, err := c.GetUserAlive(context.Background())
	if err != nil {
		t.Fatalf("GetUserAlive() error = %v", err)
	}
	if len(alive) != 2 || alive[1] != 2 || alive[3] != 4 {
		t.Fatalf("GetUserAlive() = %v, want {1:2 3:4}", alive)
	}
}
