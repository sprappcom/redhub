package redhub

import (
	"bytes"
	"sync"
	"time"

	"github.com/leslie-fei/gnettls"
	"github.com/leslie-fei/gnettls/tls"
	"github.com/panjf2000/gnet/v2"
	"github.com/sprappcom/redhub/pkg/resp"
)

type Action int

const (
	None Action = iota
	Close
	Shutdown
)

type Conn struct {
	gnet.Conn
}

type Options struct {
	Multicore        bool
	LockOSThread     bool
	ReadBufferCap    int
	LB               gnet.LoadBalancing
	NumEventLoop     int
	ReusePort        bool
	Ticker           bool
	TCPKeepAlive     time.Duration
	TCPNoDelay       gnet.TCPSocketOpt
	SocketRecvBuffer int
	SocketSendBuffer int
	TLSConfig        *tls.Config
}

func NewRedHub(
	onOpened func(c *Conn) (out []byte, action Action),
	onClosed func(c *Conn, err error) (action Action),
	handler func(cmd resp.Command, out []byte) ([]byte, Action),
) *redHub {
	return &redHub{
		redHubBufMap: make(map[gnet.Conn]*connBuffer),
		connSync:     sync.RWMutex{},
		onOpened:     onOpened,
		onClosed:     onClosed,
		handler:      handler,
		pubsub:       NewPubSub(),
	}
}

type redHub struct {
	gnet.BuiltinEventEngine
	eng          gnet.Engine
	onOpened     func(c *Conn) (out []byte, action Action)
	onClosed     func(c *Conn, err error) (action Action)
	handler      func(cmd resp.Command, out []byte) ([]byte, Action)
	redHubBufMap map[gnet.Conn]*connBuffer
	connSync     sync.RWMutex
	pubsub       *PubSub
}

type connBuffer struct {
	buf     bytes.Buffer
	command []resp.Command
}

func (rs *redHub) OnBoot(eng gnet.Engine) gnet.Action {
	rs.eng = eng
	return gnet.None
}

func (rs *redHub) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	rs.connSync.Lock()
	defer rs.connSync.Unlock()
	rs.redHubBufMap[c] = new(connBuffer)
	rs.onOpened(&Conn{Conn: c})
	return
}

func (rs *redHub) OnClose(c gnet.Conn, err error) (action gnet.Action) {
	rs.connSync.Lock()
	defer rs.connSync.Unlock()
	delete(rs.redHubBufMap, c)
	rs.onClosed(&Conn{Conn: c}, err)
	return gnet.None
}

func (rs *redHub) OnTraffic(c gnet.Conn) gnet.Action {
	rs.connSync.RLock()
	defer rs.connSync.RUnlock()
	cb, ok := rs.redHubBufMap[c]
	if !ok {
		out := resp.AppendError(nil, "ERR Client is closed")
		c.Write(out)
		return gnet.Close
	}
	frame, _ := c.Next(-1)
	cb.buf.Write(frame)
	cmds, lastbyte, err := resp.ReadCommands(cb.buf.Bytes())
	if err != nil {
		out := resp.AppendError(nil, "ERR "+err.Error())
		c.Write(out)
		return gnet.None
	}
	cb.command = append(cb.command, cmds...)
	cb.buf.Reset()
	if len(lastbyte) == 0 {
		var status Action
		var out []byte
		for len(cb.command) > 0 {
			cmd := cb.command[0]
			if len(cb.command) == 1 {
				cb.command = nil
			} else {
				cb.command = cb.command[1:]
			}
			out, status = rs.handler(cmd, out)
			c.Write(out)
			switch status {
			case Close:
				return gnet.Close
			}
		}
	} else {
		cb.buf.Write(lastbyte)
	}
	return gnet.None
}

func ListenAndServe(addr string, options Options, rh *redHub) error {
	opts := []gnet.Option{
		gnet.WithMulticore(options.Multicore),
		gnet.WithLockOSThread(options.LockOSThread),
		gnet.WithReadBufferCap(options.ReadBufferCap),
		gnet.WithLoadBalancing(options.LB),
		gnet.WithNumEventLoop(options.NumEventLoop),
		gnet.WithReusePort(options.ReusePort),
		gnet.WithTicker(options.Ticker),
		gnet.WithTCPKeepAlive(options.TCPKeepAlive),
		gnet.WithTCPNoDelay(options.TCPNoDelay),
		gnet.WithSocketRecvBuffer(options.SocketRecvBuffer),
		gnet.WithSocketSendBuffer(options.SocketSendBuffer),
	}

	if options.TLSConfig != nil {
		return gnettls.Run(rh, addr, options.TLSConfig, opts...)
	}
	return gnet.Run(rh, addr, opts...)
}

// PubSub related code

type PubSub struct {
	mu     sync.RWMutex
	chans  map[string][]Conn
}

func NewPubSub() *PubSub {
	return &PubSub{
		chans: make(map[string][]Conn),
	}
}

func (ps *PubSub) Subscribe(conn Conn, channel string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.chans[channel] = append(ps.chans[channel], conn)
}

func (ps *PubSub) Unsubscribe(conn Conn, channel string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	conns := ps.chans[channel]
	for i, c := range conns {
		if c == conn {
			ps.chans[channel] = append(conns[:i], conns[i+1:]...)
			break
		}
	}
	if len(ps.chans[channel]) == 0 {
		delete(ps.chans, channel)
	}
}

func (ps *PubSub) Publish(channel, message string) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	for _, conn := range ps.chans[channel] {
		conn.AsyncWrite([]byte(message), nil)
	}
}

func (rs *redHub) OnSubscribe(conn Conn, channel string) {
	rs.pubsub.Subscribe(conn, channel)
}

func (rs *redHub) OnUnsubscribe(conn Conn, channel string) {
	rs.pubsub.Unsubscribe(conn, channel)
}

func (rs *redHub) OnPublish(channel, message string) {
	rs.pubsub.Publish(channel, message)
}
