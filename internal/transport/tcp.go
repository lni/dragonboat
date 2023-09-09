// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package transport

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"net"
	"sync"
	"time"

	"github.com/juju/ratelimit"
	"github.com/lni/goutils/netutil"
	"github.com/lni/goutils/syncutil"

	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/internal/settings"
	"github.com/lni/dragonboat/v3/raftio"
	pb "github.com/lni/dragonboat/v3/raftpb"
)

var (
	// ErrBadMessage is the error returned to indicate the incoming message is
	// corrupted.
	ErrBadMessage       = errors.New("invalid message")
	errPoisonReceived   = errors.New("poison received")
	magicNumber         = [2]byte{0xAE, 0x7D}
	poisonNumber        = [2]byte{0x0, 0x0}
	payloadBufferSize   = settings.SnapshotChunkSize + 1024*128
	tlsHandshackTimeout = 10 * time.Second
	magicNumberDuration = 1 * time.Second
	headerDuration      = 2 * time.Second
	readDuration        = 5 * time.Second
	writeDuration       = 5 * time.Second
	keepAlivePeriod     = 10 * time.Second
	perConnBufSize      = settings.Soft.PerConnectionSendBufSize
	recvBufSize         = settings.Soft.PerConnectionRecvBufSize
)

const (
	// TCPTransportName is the name of the tcp transport module.
	TCPTransportName         = "go-tcp-transport"
	requestHeaderSize        = 18
	raftType          uint16 = 100
	snapshotType      uint16 = 200
)

type requestHeader struct {
	method uint16
	size   uint64
	crc    uint32
}

// TODO:
// TCP is never reliable [1]. dragonboat uses application layer crc32 checksum
// to help protecting raft state and log from some faulty network switches or
// buggy kernels. However, this is not necessary when TLS encryption is used.
// Update tcp.go to stop crc32 checking messages when TLS is used.
//
// [1] twitter's 2015 data corruption accident -
// https://www.evanjones.ca/checksum-failure-is-a-kernel-bug.html
// https://www.evanjones.ca/tcp-and-ethernet-checksums-fail.html
func (h *requestHeader) encode(buf []byte) []byte {
	if len(buf) < requestHeaderSize {
		panic("input buf too small")
	}
	binary.BigEndian.PutUint16(buf, h.method)
	binary.BigEndian.PutUint64(buf[2:], h.size)
	binary.BigEndian.PutUint32(buf[10:], 0)
	binary.BigEndian.PutUint32(buf[14:], h.crc)
	v := crc32.ChecksumIEEE(buf[:requestHeaderSize])
	binary.BigEndian.PutUint32(buf[10:], v)
	return buf[:requestHeaderSize]
}

func (h *requestHeader) decode(buf []byte) bool {
	if len(buf) < requestHeaderSize {
		return false
	}
	incoming := binary.BigEndian.Uint32(buf[10:])
	binary.BigEndian.PutUint32(buf[10:], 0)
	expected := crc32.ChecksumIEEE(buf[:requestHeaderSize])
	if incoming != expected {
		plog.Errorf("header crc check failed")
		return false
	}
	binary.BigEndian.PutUint32(buf[10:], incoming)
	method := binary.BigEndian.Uint16(buf)
	if method != raftType && method != snapshotType {
		plog.Errorf("invalid method type")
		return false
	}
	h.method = method
	h.size = binary.BigEndian.Uint64(buf[2:])
	h.crc = binary.BigEndian.Uint32(buf[14:])
	return true
}

// Marshaler is the interface for types that can be Marshaled.
type Marshaler interface {
	MarshalTo([]byte) (int, error)
	Size() int
}

func sendPoison(conn net.Conn, poison []byte) error {
	tt := time.Now().Add(magicNumberDuration).Add(magicNumberDuration)
	if err := conn.SetWriteDeadline(tt); err != nil {
		return err
	}
	if _, err := conn.Write(poison); err != nil {
		return err
	}
	return nil
}

func sendPoisonAck(conn net.Conn, poisonAck []byte) error {
	return sendPoison(conn, poisonAck)
}

func waitPoisonAck(conn net.Conn) {
	ack := make([]byte, len(poisonNumber))
	tt := time.Now().Add(keepAlivePeriod)
	if err := conn.SetReadDeadline(tt); err != nil {
		return
	}
	if _, err := io.ReadFull(conn, ack); err != nil {
		plog.Errorf("failed to get poison ack %v", err)
		return
	}
}

func writeMessage(conn net.Conn,
	header requestHeader, buf []byte, headerBuf []byte, encrypted bool) error {
	header.size = uint64(len(buf))
	if !encrypted {
		header.crc = crc32.ChecksumIEEE(buf)
	}
	headerBuf = header.encode(headerBuf)
	tt := time.Now().Add(magicNumberDuration).Add(headerDuration)
	if err := conn.SetWriteDeadline(tt); err != nil {
		return err
	}
	if _, err := conn.Write(magicNumber[:]); err != nil {
		return err
	}
	if _, err := conn.Write(headerBuf); err != nil {
		return err
	}
	sent := 0
	bufSize := int(recvBufSize)
	for sent < len(buf) {
		if sent+bufSize > len(buf) {
			bufSize = len(buf) - sent
		}
		tt = time.Now().Add(writeDuration)
		if err := conn.SetWriteDeadline(tt); err != nil {
			return err
		}
		if _, err := conn.Write(buf[sent : sent+bufSize]); err != nil {
			return err
		}
		sent += bufSize
	}
	if sent != len(buf) {
		plog.Panicf("sent %d, buf len %d", sent, len(buf))
	}
	return nil
}

func readMessage(conn net.Conn,
	header []byte, rbuf []byte, encrypted bool) (requestHeader, []byte, error) {
	tt := time.Now().Add(headerDuration)
	if err := conn.SetReadDeadline(tt); err != nil {
		return requestHeader{}, nil, err
	}
	if _, err := io.ReadFull(conn, header); err != nil {
		plog.Errorf("failed to get the header")
		return requestHeader{}, nil, err
	}
	rheader := requestHeader{}
	if !rheader.decode(header) {
		plog.Errorf("invalid header")
		return requestHeader{}, nil, ErrBadMessage
	}
	if rheader.size == 0 {
		plog.Errorf("invalid payload length")
		return requestHeader{}, nil, ErrBadMessage
	}
	var buf []byte
	if rheader.size > uint64(len(rbuf)) {
		buf = make([]byte, rheader.size)
	} else {
		buf = rbuf[:rheader.size]
	}
	received := uint64(0)
	var recvBuf []byte
	if rheader.size < recvBufSize {
		recvBuf = buf[:rheader.size]
	} else {
		recvBuf = buf[:recvBufSize]
	}
	toRead := rheader.size
	for toRead > 0 {
		tt = time.Now().Add(readDuration)
		if err := conn.SetReadDeadline(tt); err != nil {
			return requestHeader{}, nil, err
		}
		if _, err := io.ReadFull(conn, recvBuf); err != nil {
			return requestHeader{}, nil, err
		}
		toRead -= uint64(len(recvBuf))
		received += uint64(len(recvBuf))
		if toRead < recvBufSize {
			recvBuf = buf[received : received+toRead]
		} else {
			recvBuf = buf[received : received+recvBufSize]
		}
	}
	if received != rheader.size {
		panic("unexpected size")
	}
	if !encrypted && crc32.ChecksumIEEE(buf) != rheader.crc {
		plog.Errorf("invalid payload checksum")
		return requestHeader{}, nil, ErrBadMessage
	}
	return rheader, buf, nil
}

func readMagicNumber(conn net.Conn, magicNum []byte) error {
	tt := time.Now().Add(magicNumberDuration)
	if err := conn.SetReadDeadline(tt); err != nil {
		return err
	}
	if _, err := io.ReadFull(conn, magicNum); err != nil {
		return err
	}
	if bytes.Equal(magicNum, poisonNumber[:]) {
		return errPoisonReceived
	}
	if !bytes.Equal(magicNum, magicNumber[:]) {
		return ErrBadMessage
	}
	return nil
}

type connection struct {
	conn net.Conn
	lr   io.Reader
	lw   io.Writer
}

func newConnection(conn net.Conn,
	rb *ratelimit.Bucket, wb *ratelimit.Bucket) net.Conn {
	c := &connection{conn: conn}
	if rb != nil {
		c.lr = ratelimit.Reader(conn, rb)
	}
	if wb != nil {
		c.lw = ratelimit.Writer(conn, wb)
	}
	return c
}

func (c *connection) Close() error {
	return c.conn.Close()
}

func (c *connection) Read(b []byte) (int, error) {
	if c.lr != nil {
		return c.lr.Read(b)
	}
	return c.conn.Read(b)
}

func (c *connection) Write(b []byte) (int, error) {
	if c.lw != nil {
		return c.lw.Write(b)
	}
	return c.conn.Write(b)
}

func (c *connection) LocalAddr() net.Addr {
	panic("not implemented")
}

func (c *connection) RemoteAddr() net.Addr {
	panic("not implemented")
}

func (c *connection) SetDeadline(t time.Time) error {
	return c.conn.SetDeadline(t)
}

func (c *connection) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

func (c *connection) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}

// TCPConnection is the connection used for sending raft messages to remote
// nodes.
type TCPConnection struct {
	conn      net.Conn
	header    []byte
	payload   []byte
	encrypted bool
}

var _ raftio.IConnection = (*TCPConnection)(nil)

// NewTCPConnection creates and returns a new TCPConnection instance.
func NewTCPConnection(conn net.Conn,
	rb *ratelimit.Bucket, wb *ratelimit.Bucket, encrypted bool) *TCPConnection {
	return &TCPConnection{
		conn:      newConnection(conn, rb, wb),
		header:    make([]byte, requestHeaderSize),
		payload:   make([]byte, perConnBufSize),
		encrypted: encrypted,
	}
}

// Close closes the TCPConnection instance.
func (c *TCPConnection) Close() {
	if err := c.conn.Close(); err != nil {
		plog.Errorf("failed to close the connection %v", err)
	}
}

// SendMessageBatch sends a raft message batch to remote node.
func (c *TCPConnection) SendMessageBatch(batch pb.MessageBatch) error {
	header := requestHeader{method: raftType}
	sz := batch.SizeUpperLimit()
	var buf []byte
	if len(c.payload) < sz {
		buf = make([]byte, sz)
	} else {
		buf = c.payload
	}
	n, err := batch.MarshalTo(buf)
	if err != nil {
		panic(err)
	}
	return writeMessage(c.conn, header, buf[:n], c.header, c.encrypted)
}

// TCPSnapshotConnection is the connection for sending raft snapshot chunks to
// remote nodes.
type TCPSnapshotConnection struct {
	conn      net.Conn
	header    []byte
	encrypted bool
}

var _ raftio.ISnapshotConnection = (*TCPSnapshotConnection)(nil)

// NewTCPSnapshotConnection creates and returns a new snapshot connection.
func NewTCPSnapshotConnection(conn net.Conn,
	rb *ratelimit.Bucket, wb *ratelimit.Bucket,
	encrypted bool) *TCPSnapshotConnection {
	return &TCPSnapshotConnection{
		conn:      newConnection(conn, rb, wb),
		header:    make([]byte, requestHeaderSize),
		encrypted: encrypted,
	}
}

// Close closes the snapshot connection.
func (c *TCPSnapshotConnection) Close() {
	defer c.conn.Close()
	if err := sendPoison(c.conn, poisonNumber[:]); err != nil {
		return
	}
	waitPoisonAck(c.conn)
}

// SendChunk sends the specified snapshot chunk to remote node.
func (c *TCPSnapshotConnection) SendChunk(chunk pb.Chunk) error {
	header := requestHeader{method: snapshotType}
	sz := chunk.Size()
	buf := make([]byte, sz)
	n, err := chunk.MarshalTo(buf)
	if err != nil {
		panic(err)
	}
	return writeMessage(c.conn, header, buf[:n], c.header, c.encrypted)
}

// TCP is a TCP based transport module for exchanging raft messages and
// snapshots between NodeHost instances.
type TCP struct {
	nhConfig       config.NodeHostConfig
	stopper        *syncutil.Stopper
	connStopper    *syncutil.Stopper
	requestHandler raftio.MessageHandler
	chunkHandler   raftio.ChunkHandler
	encrypted      bool
	readBucket     *ratelimit.Bucket
	writeBucket    *ratelimit.Bucket
}

var _ raftio.ITransport = (*TCP)(nil)

// NewTCPTransport creates and returns a new TCP transport module.
func NewTCPTransport(nhConfig config.NodeHostConfig,
	requestHandler raftio.MessageHandler,
	chunkHandler raftio.ChunkHandler) raftio.ITransport {
	t := &TCP{
		nhConfig:       nhConfig,
		stopper:        syncutil.NewStopper(),
		connStopper:    syncutil.NewStopper(),
		requestHandler: requestHandler,
		chunkHandler:   chunkHandler,
		encrypted:      nhConfig.MutualTLS,
	}
	rate := nhConfig.MaxSnapshotSendBytesPerSecond
	if rate > 0 {
		t.writeBucket = ratelimit.NewBucketWithRate(float64(rate), int64(rate)*2)
	}
	rate = nhConfig.MaxSnapshotRecvBytesPerSecond
	if rate > 0 {
		t.readBucket = ratelimit.NewBucketWithRate(float64(rate), int64(rate)*2)
	}
	return t
}

// Start starts the TCP transport module.
func (t *TCP) Start() error {
	address := t.nhConfig.GetListenAddress()
	tlsConfig, err := t.nhConfig.GetServerTLSConfig()
	if err != nil {
		return err
	}
	listener, err := netutil.NewStoppableListener(address,
		tlsConfig, t.stopper.ShouldStop())
	if err != nil {
		return err
	}
	t.connStopper.RunWorker(func() {
		// sync.WaitGroup's doc mentions that
		// "Note that calls with a positive delta that occur when the counter is
		//  zero must happen before a Wait."
		// It is unclear that whether the stdlib is going complain in future
		// releases when Wait() is called when the counter is zero and Add() with
		// positive delta has never been called.
		<-t.connStopper.ShouldStop()
	})
	t.stopper.RunWorker(func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				if err == netutil.ErrListenerStopped {
					return
				}
				panic(err)
			}
			var once sync.Once
			connCloseCh := make(chan struct{})
			closeFn := func() {
				once.Do(func() {
					select {
						case connCloseCh <- struct{}{}:
						default:
					}
					if err := conn.Close(); err != nil {
						plog.Errorf("failed to close the connection %v", err)
					}
				})
			}
			t.connStopper.RunWorker(func() {
				select {
				case <-t.stopper.ShouldStop():
				case <-connCloseCh:
				}
				closeFn()
			})
			t.connStopper.RunWorker(func() {
				t.serveConn(conn)
				closeFn()
			})
		}
	})
	return nil
}

// Stop stops the TCP transport module.
func (t *TCP) Stop() {
	t.stopper.Stop()
	t.connStopper.Stop()
}

// GetConnection returns a new raftio.IConnection for sending raft messages.
func (t *TCP) GetConnection(ctx context.Context,
	target string) (raftio.IConnection, error) {
	conn, err := t.getConnection(ctx, target)
	if err != nil {
		return nil, err
	}
	return NewTCPConnection(conn, nil, nil, t.encrypted), nil
}

// GetSnapshotConnection returns a new raftio.IConnection for sending raft
// snapshots.
func (t *TCP) GetSnapshotConnection(ctx context.Context,
	target string) (raftio.ISnapshotConnection, error) {
	conn, err := t.getConnection(ctx, target)
	if err != nil {
		return nil, err
	}
	return NewTCPSnapshotConnection(conn,
		t.readBucket, t.writeBucket, t.encrypted), nil
}

// Name returns a human readable name of the TCP transport module.
func (t *TCP) Name() string {
	return TCPTransportName
}

func (t *TCP) serveConn(conn net.Conn) {
	magicNum := make([]byte, len(magicNumber))
	header := make([]byte, requestHeaderSize)
	tbuf := make([]byte, payloadBufferSize)
	for {
		err := readMagicNumber(conn, magicNum)
		if err != nil {
			if err == errPoisonReceived {
				_ = sendPoisonAck(conn, poisonNumber[:])
				return
			}
			if err == ErrBadMessage {
				return
			}
			operr, ok := err.(net.Error)
			if ok && operr.Timeout() {
				continue
			} else {
				return
			}
		}
		rheader, buf, err := readMessage(conn, header, tbuf, t.encrypted)
		if err != nil {
			return
		}
		if rheader.method == raftType {
			batch := pb.MessageBatch{}
			if err := batch.Unmarshal(buf); err != nil {
				return
			}
			t.requestHandler(batch)
		} else {
			chunk := pb.Chunk{}
			if err := chunk.Unmarshal(buf); err != nil {
				return
			}
			if !t.chunkHandler(chunk) {
				plog.Errorf("chunk rejected %s", chunkKey(chunk))
				return
			}
		}
	}
}

func setTCPConn(conn *net.TCPConn) error {
	if err := conn.SetLinger(0); err != nil {
		return err
	}
	if err := conn.SetKeepAlive(true); err != nil {
		return err
	}
	return conn.SetKeepAlivePeriod(keepAlivePeriod)
}

// FIXME:
// context.Context is ignored
func (t *TCP) getConnection(ctx context.Context,
	target string) (net.Conn, error) {
	timeout := time.Duration(dialTimeoutSecond) * time.Second
	conn, err := net.DialTimeout("tcp", target, timeout)
	if err != nil {
		return nil, err
	}
	tcpconn, ok := conn.(*net.TCPConn)
	if ok {
		if err := setTCPConn(tcpconn); err != nil {
			return nil, err
		}
	}
	tlsConfig, err := t.nhConfig.GetClientTLSConfig(target)
	if err != nil {
		return nil, err
	}
	if tlsConfig != nil {
		conn = tls.Client(conn, tlsConfig)
		tt := time.Now().Add(tlsHandshackTimeout)
		if err := conn.SetDeadline(tt); err != nil {
			return nil, err
		}
		if err := conn.(*tls.Conn).Handshake(); err != nil {
			return nil, err
		}
	}
	return conn, nil
}
