package netconnutil

import (
	"context"
	"encoding/binary"
	"github.com/pkg/errors"
	"github.com/xiaonanln/pktconn"
	"io"
	"log"
	"math/rand"
	"net"
	"runtime"
	"sync"
	"testing"
)

const (
	testClientCount = 10
	sendValMax      = 100
	sendDataSize    = 1024
)

type testServer struct {
	ctx              context.Context
	buffered         bool
	snappy           bool
	injectTempErrors bool
}

// ServeTCP serves on specified address as TCP server
func (ts *testServer) serve(listenAddr string, serverReady chan struct{}, serverClosed chan struct{}) error {
	defer close(serverClosed)

	ln, err := net.Listen("tcp", listenAddr)
	log.Printf("Listening on TCP: %s ...", listenAddr)

	if err != nil {
		return err
	}

	defer ln.Close()
	close(serverReady)

	go func() {
		<-ts.ctx.Done()
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if pktconn.IsTemporary(err) {
				runtime.Gosched()
				continue
			} else {
				return err
			}
		}

		log.Printf("%s connected", conn.RemoteAddr())
		go ts.serveTCPConn(conn)
	}
}

func (ts *testServer) serveTCPConn(conn net.Conn) {

	conn = newInjectTempErrorConn(conn)
	conn = NewNoTempErrorConn(conn)

	if ts.snappy {
		conn = NewSnappyConn(conn)
	}

	if ts.buffered {
		conn = NewBufferedConn(conn, 8192, 8192)
	}

	var expectVal uint64 = 0
	var b [sendDataSize]byte
	for {
		n, err := io.ReadFull(conn, b[:])
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Panicf("ReadFull failed: err=%v", err)
		}

		val := binary.BigEndian.Uint64(b[:8])
		log.Printf("read val = %v", val)
		if val != expectVal {
			panic(errors.Errorf("expect %v, but recv %v", expectVal, val))
		}

		n, err = conn.Write(b[:])
		if err != nil {
			log.Panicf("conn.Write failed: n=%v, err=%v", n, err)
		}
		if err = tryFlush(conn); err != nil {
			panic(err)
		}

		expectVal += 1
	}
}

type injectTempErrorConn struct {
	net.Conn
}

func newInjectTempErrorConn(conn net.Conn) net.Conn {
	return &injectTempErrorConn{
		Conn: conn,
	}
}

type tempError struct {
}

func (te tempError) Error() string {
	return "temp error"
}

func (te tempError) Temporary() bool {
	return true
}

func (ec injectTempErrorConn) Read(b []byte) (n int, err error) {
	if rand.Float32() < 0.1 {
		return 0, tempError{}
	}
	return ec.Conn.Read(b)
}

func (ec injectTempErrorConn) Write(b []byte) (n int, err error) {
	return ec.Conn.Write(b)
}

func testConn(t *testing.T, buffered bool, snappy bool, injectTempErrors bool) {
	ctx, cancelCtx := context.WithCancel(context.Background())
	server := &testServer{
		ctx:              ctx,
		buffered:         buffered,
		snappy:           snappy,
		injectTempErrors: injectTempErrors,
	}
	serverReady := make(chan struct{})
	serverClosed := make(chan struct{})
	go server.serve("localhost:12345", serverReady, serverClosed)
	<-serverReady

	var waitClients sync.WaitGroup
	waitClients.Add(testClientCount)

	for i := 0; i < testClientCount; i++ {
		go func() {
			defer waitClients.Done()

			cli, err := net.Dial("tcp", "localhost:12345")
			if err != nil {
				panic(err)
			}

			if injectTempErrors {
				cli = newInjectTempErrorConn(cli)
			}

			cli = NewNoTempErrorConn(cli)

			if snappy {
				cli = NewSnappyConn(cli)
			}

			if buffered {
				cli = NewBufferedConn(cli, 8192, 8192)
			}

			var b [sendDataSize]byte
			for i := 0; i < sendDataSize; i++ {
				b[i] = 'A'
			}
			for val := 0; val <= sendValMax; val++ {
				binary.BigEndian.PutUint64(b[:8], uint64(val))
				n, err := cli.Write(b[:])
				if n != sendDataSize || err != nil {
					log.Panicf("send fail: n=%v, err=%v", n, err)
				}

				if err = tryFlush(cli); err != nil {
					panic(err)
				}

				n, err = io.ReadFull(cli, b[:])
				if err != nil {
					log.Panicf("cli ReadFull fail: n=%v, err=%v", n, err)
				}
				if binary.BigEndian.Uint64(b[:8]) != uint64(val) {
					log.Panicf("expect echo %v, but recv %v", val, binary.BigEndian.Uint64(b[:8]))
				}
			}

			cli.Close()
		}()
	}
	waitClients.Wait()

	cancelCtx()
	<-serverClosed
}

func tryFlush(conn net.Conn) error {
	if f, ok := conn.(Flushable); ok {
		return f.Flush()
	} else {
		return nil
	}
}

func TestConnNoBufferedNoSnappy(t *testing.T) {
	testConn(t, false, false, false)
}

func TestConnBufferedNoSnappy(t *testing.T) {
	testConn(t, true, false, false)
}

func TestConnNoBufferedSnappy(t *testing.T) {
	testConn(t, false, true, false)
}

func TestConnBufferedSnappy(t *testing.T) {
	testConn(t, true, true, false)
}

func TestConnBufferedSnappyIE(t *testing.T) {
	testConn(t, true, true, true)
}
