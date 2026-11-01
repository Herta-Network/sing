package socks

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

type contextTestDialer struct {
	conn net.Conn
}

func (d *contextTestDialer) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	return d.conn, nil
}

func (*contextTestDialer) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, errors.New("unsupported")
}

func TestClientDialContextCancelsHandshake(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()
	client := NewClient(
		&contextTestDialer{conn: clientConn},
		M.ParseSocksaddr("127.0.0.1:1080"),
		Version5,
		"",
		"",
	)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		conn, err := client.DialContext(ctx, N.NetworkTCP, M.ParseSocksaddr("example.com:443"))
		if conn != nil {
			_ = conn.Close()
		}
		result <- err
	}()

	var greeting [3]byte
	if _, err := io.ReadFull(serverConn, greeting[:]); err != nil {
		t.Fatal(err)
	}
	cancel()

	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("DialContext() error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("DialContext() did not stop after context cancellation")
	}
}

func TestClientBindContextCancelsHandshake(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()
	client := NewClient(&contextTestDialer{conn: clientConn}, M.ParseSocksaddr("127.0.0.1:1080"), Version5, "", "")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { _, err := client.BindContext(ctx, M.ParseSocksaddr("example.com:443")); result <- err }()
	var greeting [3]byte
	if _, err := io.ReadFull(serverConn, greeting[:]); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("bind handshake did not cancel")
	}
}

func TestCompletedHandshakeSurvivesLaterCancellation(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()
	deadline := time.Now().Add(time.Second)
	clientConn.SetDeadline(deadline)
	serverConn.SetDeadline(deadline)
	client := NewClient(&contextTestDialer{conn: clientConn}, M.ParseSocksaddr("127.0.0.1:1080"), Version5, "", "")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serverDone := make(chan error, 1)
	go func() {
		var greeting [3]byte
		if _, err := io.ReadFull(serverConn, greeting[:]); err != nil {
			serverDone <- err
			return
		}
		if _, err := serverConn.Write([]byte{5, 0}); err != nil {
			serverDone <- err
			return
		}
		// CONNECT to 127.0.0.1:443 uses a ten-byte request.
		var request [10]byte
		if _, err := io.ReadFull(serverConn, request[:]); err != nil {
			serverDone <- err
			return
		}
		_, err := serverConn.Write([]byte{5, 0, 0, 1, 127, 0, 0, 1, 0, 0})
		if err != nil {
			serverDone <- err
			return
		}
		var data [1]byte
		_, err = io.ReadFull(serverConn, data[:])
		serverDone <- err
	}()
	conn, err := client.DialContext(ctx, N.NetworkTCP, M.ParseSocksaddr("127.0.0.1:443"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	cancel()
	if _, err := conn.Write([]byte{1}); err != nil {
		t.Fatalf("connection closed after successful handshake: %v", err)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}
