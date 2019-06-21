// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other Dragonboat authors.
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

	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/internal/settings"
	"github.com/lni/dragonboat/v3/internal/utils/netutil"
	"github.com/lni/dragonboat/v3/internal/utils/syncutil"
	"github.com/lni/dragonboat/v3/raftio"
	"github.com/lni/dragonboat/v3/raftpb"
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
	keepAlivePeriod     = 30 * time.Second
	perConnBufSize      = settings.Soft.PerConnectionBufferSize
	recvBufSize         = settings.Soft.PerCpnnectionRecvBufSize
)

const (
	// TCPRaftRPCName is the name of the tcp RPC module.
	TCPRaftRPCName           = "go-tcp-transport"
	requestHeaderSize        = 14
	raftType          uint16 = 100
	snapshotType      uint16 = 200
)

type requestHeader struct {
	method uint16
	size   uint32
	crc    uint32
}

func (h *requestHeader) encode(buf []byte) []byte {
	if len(buf) < requestHeaderSize {
		panic("input buf too small")
	}
	binary.BigEndian.PutUint16(buf, h.method)
	binary.BigEndian.PutUint32(buf[2:], h.size)
	binary.BigEndian.PutUint32(buf[6:], 0)
	binary.BigEndian.PutUint32(buf[10:], h.crc)
	v := crc32.ChecksumIEEE(buf[:requestHeaderSize])
	binary.BigEndian.PutUint32(buf[6:], v)
	return buf[:requestHeaderSize]
}

func (h *requestHeader) decode(buf []byte) bool {
	if len(buf) < requestHeaderSize {
		return false
	}
	incoming := binary.BigEndian.Uint32(buf[6:])
	binary.BigEndian.PutUint32(buf[6:], 0)
	expected := crc32.ChecksumIEEE(buf[:requestHeaderSize])
	if incoming != expected {
		plog.Errorf("header crc check failed")
		return false
	}
	binary.BigEndian.PutUint32(buf[6:], incoming)
	method := binary.BigEndian.Uint16(buf)
	if method != raftType && method != snapshotType {
		plog.Errorf("invalid method type")
		return false
	}
	h.method = method
	h.size = binary.BigEndian.Uint32(buf[2:])
	h.crc = binary.BigEndian.Uint32(buf[10:])
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
	header requestHeader, buf []byte, headerBuf []byte) error {
	crc := crc32.ChecksumIEEE(buf)
	header.size = uint32(len(buf))
	header.crc = crc
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
	header []byte, rbuf []byte) (requestHeader, []byte, error) {
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
	if rheader.size > uint32(len(rbuf)) {
		buf = make([]byte, rheader.size)
	} else {
		buf = rbuf[:rheader.size]
	}
	received := 0
	var recvBuf []byte
	if rheader.size < uint32(recvBufSize) {
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
		toRead -= uint32(len(recvBuf))
		received += len(recvBuf)
		if toRead < uint32(recvBufSize) {
			recvBuf = buf[received : uint32(received)+toRead]
		} else {
			recvBuf = buf[received : received+int(recvBufSize)]
		}
	}
	if uint32(received) != rheader.size {
		panic("unexpected size")
	}
	if crc32.ChecksumIEEE(buf) != rheader.crc {
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

// TCPConnection is the connection used for sending raft messages to remote
// nodes.
type TCPConnection struct {
	conn    net.Conn
	header  []byte
	payload []byte
}

// NewTCPConnection creates and returns a new TCPConnection instance.
func NewTCPConnection(conn net.Conn) *TCPConnection {
	return &TCPConnection{
		conn:    conn,
		header:  make([]byte, requestHeaderSize),
		payload: make([]byte, perConnBufSize),
	}
}

// Close closes the TCPConnection instance.
func (c *TCPConnection) Close() {
	if err := c.conn.Close(); err != nil {
		plog.Errorf("failed to close the connection %v", err)
	}
}

// SendMessageBatch sends a raft message batch to remote node.
func (c *TCPConnection) SendMessageBatch(batch raftpb.MessageBatch) error {
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
	return writeMessage(c.conn, header, buf[:n], c.header)
}

// TCPSnapshotConnection is the connection for sending raft snapshot chunks to
// remote nodes.
type TCPSnapshotConnection struct {
	conn   net.Conn
	header []byte
}

// NewTCPSnapshotConnection creates and returns a new snapshot connection.
func NewTCPSnapshotConnection(conn net.Conn) *TCPSnapshotConnection {
	return &TCPSnapshotConnection{
		conn:   conn,
		header: make([]byte, requestHeaderSize),
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

// SendSnapshotChunk sends the specified snapshot chunk to remote node.
func (c *TCPSnapshotConnection) SendSnapshotChunk(chunk raftpb.SnapshotChunk) error {
	header := requestHeader{method: snapshotType}
	sz := chunk.Size()
	buf := make([]byte, sz)
	n, err := chunk.MarshalTo(buf)
	if err != nil {
		panic(err)
	}
	return writeMessage(c.conn, header, buf[:n], c.header)
}

// TCPTransport is a TCP based RPC module for exchanging raft messages and
// snapshots between NodeHost instances.
type TCPTransport struct {
	nhConfig       config.NodeHostConfig
	stopper        *syncutil.Stopper
	requestHandler raftio.RequestHandler
	chunks         raftio.IChunkSink
}

// NewTCPTransport creates and returns a new TCP transport module.
func NewTCPTransport(nhConfig config.NodeHostConfig,
	requestHandler raftio.RequestHandler,
	sinkFactory raftio.ChunkSinkFactory) raftio.IRaftRPC {
	chunks := sinkFactory()
	stopper := syncutil.NewStopper()
	stopper.RunWorker(func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				chunks.Tick()
			case <-stopper.ShouldStop():
				return
			}
		}
	})
	return &TCPTransport{
		nhConfig:       nhConfig,
		stopper:        stopper,
		requestHandler: requestHandler,
		chunks:         chunks,
	}
}

// Start starts the TCP transport module.
func (g *TCPTransport) Start() error {
	address := g.nhConfig.GetListenAddress()
	tlsConfig, err := g.nhConfig.GetServerTLSConfig()
	if err != nil {
		return err
	}
	listener, err := netutil.NewStoppableListener(address,
		tlsConfig, g.stopper.ShouldStop())
	if err != nil {
		return err
	}
	g.stopper.RunWorker(func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				if err == netutil.ErrListenerStopped {
					return
				}
				panic(err)
			}
			var once sync.Once
			closeFn := func() {
				once.Do(func() {
					if err := conn.Close(); err != nil {
						plog.Errorf("failed to close the connection %v", err)
					}
				})
			}
			g.stopper.RunWorker(func() {
				<-g.stopper.ShouldStop()
				closeFn()
			})
			g.stopper.RunWorker(func() {
				g.serveConn(conn)
				closeFn()
			})
		}
	})
	return nil
}

// Stop stops the TCP transport module.
func (g *TCPTransport) Stop() {
	g.stopper.Stop()
	g.chunks.Close()
}

// GetConnection returns a new raftio.IConnection for sending raft messages.
func (g *TCPTransport) GetConnection(ctx context.Context,
	target string) (raftio.IConnection, error) {
	conn, err := g.getConnection(ctx, target)
	if err != nil {
		return nil, err
	}
	return NewTCPConnection(conn), nil
}

// GetSnapshotConnection returns a new raftio.IConnection for sending raft
// snapshots.
func (g *TCPTransport) GetSnapshotConnection(ctx context.Context,
	target string) (raftio.ISnapshotConnection, error) {
	conn, err := g.getConnection(ctx, target)
	if err != nil {
		return nil, err
	}
	return NewTCPSnapshotConnection(conn), nil
}

// Name returns a human readable name of the TCP transport module.
func (g *TCPTransport) Name() string {
	return TCPRaftRPCName
}

func (g *TCPTransport) serveConn(conn net.Conn) {
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
		rheader, buf, err := readMessage(conn, header, tbuf)
		if err != nil {
			return
		}
		if rheader.method == raftType {
			batch := raftpb.MessageBatch{}
			if err := batch.Unmarshal(buf); err != nil {
				return
			}
			g.requestHandler(batch)
		} else {
			chunk := raftpb.SnapshotChunk{}
			if err := chunk.Unmarshal(buf); err != nil {
				return
			}
			if !g.chunks.AddChunk(chunk) {
				plog.Errorf("chunk rejected %s", snapshotKey(chunk))
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

func (g *TCPTransport) getConnection(ctx context.Context,
	target string) (net.Conn, error) {
	timeout := time.Duration(getDialTimeoutSecond()) * time.Second
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
	tlsConfig, err := g.nhConfig.GetClientTLSConfig(target)
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
