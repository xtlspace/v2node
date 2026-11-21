package core

import (
	"context"
	"io"
	stdnet "net"
	"sync"
	"time"

	"github.com/xtls/xray-core/common/buf"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/transport"
)

// DialContext dials address through the xray dispatcher so that HTTP requests
// issued by the node process itself are subject to the configured routing rules
// and the default outbound.
func (v *V2Core) DialContext(ctx context.Context, network, address string) (stdnet.Conn, error) {
	dest, err := xnet.ParseDestination(network + ":" + address)
	if err != nil {
		return nil, err
	}
	sctx := session.ContextWithOutbounds(ctx, []*session.Outbound{{}})
	link, err := v.dispatcher.Dispatch(sctx, dest)
	if err != nil {
		return nil, err
	}
	return newLinkConn(link, dest), nil
}

// linkConn adapts a transport.Link to net.Conn using a pair of io.Pipes.
//
//	link.Reader --(buf.Copy)--> pipe --> Read()
//	Write() --> pipe --(buf.Copy)--> link.Writer
type linkConn struct {
	pr  *io.PipeReader // read side: Link.Reader -> pipe -> Read()
	pw  *io.PipeWriter
	pr2 *io.PipeReader // write side: Write() -> pipe -> Link.Writer
	pw2 *io.PipeWriter
	link   *transport.Link
	remote xnet.Destination
	once   sync.Once
}

func newLinkConn(link *transport.Link, remote xnet.Destination) *linkConn {
	pr, pw := io.Pipe()
	pr2, pw2 := io.Pipe()
	c := &linkConn{pr: pr, pw: pw, pr2: pr2, pw2: pw2, link: link, remote: remote}
	go func() {
		_ = buf.Copy(link.Reader, buf.NewWriter(pw))
		pw.Close()
	}()
	go func() {
		_ = buf.Copy(buf.NewReader(pr2), link.Writer)
		pr2.Close()
	}()
	return c
}

func (c *linkConn) Read(p []byte) (int, error)  { return c.pr.Read(p) }
func (c *linkConn) Write(p []byte) (int, error) { return c.pw2.Write(p) }

func (c *linkConn) Close() error {
	c.once.Do(func() {
		c.pw.Close()
		c.pr2.Close()
		c.pr.Close()
		c.pw2.Close()
	})
	return nil
}

func (c *linkConn) LocalAddr() stdnet.Addr { return &stdnet.TCPAddr{} }

func (c *linkConn) RemoteAddr() stdnet.Addr {
	if a := c.remote.RawNetAddr(); a != nil {
		return a
	}
	return &stdnet.TCPAddr{}
}

func (c *linkConn) SetDeadline(time.Time) error      { return nil }
func (c *linkConn) SetReadDeadline(time.Time) error  { return nil }
func (c *linkConn) SetWriteDeadline(time.Time) error { return nil }
