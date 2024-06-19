package main

import (
	"flag"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/leslie-fei/gnettls"
	"github.com/leslie-fei/gnettls/tls"
	"github.com/panjf2000/gnet/v2"
	"github.com/sprappcom/redhub"
	"github.com/sprappcom/redhub/pkg/resp"
	cxsysinfodebug "github.com/cloudxaas/gosysinfo/debug"
)

var addr = ":6380"

func main() {
	go cxsysinfodebug.LogMemStatsPeriodically(1*time.Second, &cxsysinfodebug.FileDescriptorTracker{})

	var mu sync.RWMutex
	var items = make(map[string][]byte)
	ps := redhub.NewPubSub() // Initialize PubSub
	var network string
	var multicore bool
	var reusePort bool
	var tlsEnabled bool
	var certFile string
	var keyFile string

	flag.StringVar(&network, "network", "tcp", "server network (default \"tcp\")")
	flag.StringVar(&addr, "addr", "127.0.0.1:6380", "server addr (default \":6380\")")
	flag.BoolVar(&multicore, "multicore", false, "multicore")
	flag.BoolVar(&reusePort, "reusePort", true, "reusePort")
	flag.BoolVar(&tlsEnabled, "tls", true, "enable TLS")
	flag.StringVar(&certFile, "cert", "server.crt", "TLS certificate file")
	flag.StringVar(&keyFile, "key", "server.key", "TLS key file")
	flag.Parse()

	protoAddr := fmt.Sprintf("%s://%s", network, addr)
	option := redhub.Options{
		Multicore: multicore,
		ReusePort: reusePort,
	}

	if tlsEnabled {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			log.Fatalf("failed to load TLS certificates: %v", err)
		}
		option.TLSConfig = &tls.Config{Certificates: []tls.Certificate{cert}}
	}

	handler := func(cmd resp.Command, out []byte) ([]byte, redhub.Action) {
		var status redhub.Action
		conn, ok := cmd.Conn.(*redhub.Conn) // Retrieve the connection reference
		if !ok {
			return resp.AppendError(out, "ERR invalid connection"), redhub.Close
		}
		switch strings.ToLower(string(cmd.Args[0])) {
		default:
			out = resp.AppendError(out, "ERR unknown command '"+string(cmd.Args[0])+"'")
		case "ping":
			out = resp.AppendString(out, "PONG")
		case "quit":
			out = resp.AppendString(out, "OK")
			status = redhub.Close
		case "set":
			if len(cmd.Args) != 3 {
				out = resp.AppendError(out, "ERR wrong number of arguments for '"+string(cmd.Args[0])+"' command")
				break
			}
			mu.Lock()
			items[string(cmd.Args[1])] = cmd.Args[2]
			mu.Unlock()
			out = resp.AppendString(out, "OK")
		case "get":
			if len(cmd.Args) != 2 {
				out = resp.AppendError(out, "ERR wrong number of arguments for '"+string(cmd.Args[0])+"' command")
				break
			}
			mu.RLock()
			val, ok := items[string(cmd.Args[1])]
			mu.RUnlock()
			if !ok {
				out = resp.AppendNull(out)
			} else {
				out = resp.AppendBulk(out, val)
			}
		case "del":
			if len(cmd.Args) != 2 {
				out = resp.AppendError(out, "ERR wrong number of arguments for '"+string(cmd.Args[0])+"' command")
				break
			}
			mu.Lock()
			_, ok := items[string(cmd.Args[1])]
			delete(items, string(cmd.Args[1]))
			mu.Unlock()
			if !ok {
				out = resp.AppendInt(out, 0)
			} else {
				out = resp.AppendInt(out, 1)
			}
		case "config":
			out = resp.AppendArray(out, 2)
			out = resp.AppendBulk(out, cmd.Args[2])
			out = resp.AppendBulkString(out, "")
		case "publish":
			if len(cmd.Args) != 3 {
				out = resp.AppendError(out, "ERR wrong number of arguments for '"+string(cmd.Args[0])+"' command")
				break
			}
			ps.Publish(string(cmd.Args[1]), string(cmd.Args[2]))
			out = resp.AppendInt(out, 1)
		case "subscribe":
			if len(cmd.Args) < 2 {
				out = resp.AppendError(out, "ERR wrong number of arguments for '"+string(cmd.Args[0])+"' command")
				break
			}
			for i := 1; i < len(cmd.Args); i++ {
				ps.Subscribe(conn, string(cmd.Args[i]))
			}
			status = redhub.None // Ensure connection is kept open for subscriptions
		}
		return out, status
	}

	rh := redhub.NewRedHub(
		func(c *redhub.Conn) (out []byte, action redhub.Action) {
			return
		},
		func(c *redhub.Conn, err error) (action redhub.Action) {
			return
		},
		handler,
	)

	log.Printf("started redhub server at %s", addr)
	var err error
	if tlsEnabled {
		err = gnettls.Run(rh, protoAddr, option.TLSConfig,
			gnet.WithMulticore(option.Multicore),
			gnet.WithReusePort(option.ReusePort),
		)
	} else {
		err = redhub.ListenAndServe(protoAddr, option, rh)
	}
	if err != nil {
		log.Fatal(err)
	}
}
