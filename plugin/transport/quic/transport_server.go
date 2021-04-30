package quic

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"hash/crc32"
	"io"
	"math/big"
	"time"

	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/logger"
	"github.com/lni/dragonboat/v3/raftio"
	"github.com/lni/dragonboat/v3/raftpb"
	"github.com/lni/goutils/syncutil"
	"github.com/lucas-clemente/quic-go"
)

var (
	// ErrBadMessage is the error returned to indicate the incoming message is
	// corrupted.
	ErrBadMessage       = errors.New("invalid message")
	errPoisonReceived   = errors.New("poison received")
	magicNumber         = [2]byte{0xAE, 0x7D}
	poisonNumber        = [2]byte{0x0, 0x0}
	payloadBufferSize   = 2*1024*1024 + 1024*128
	tlsHandshakeTimeout = 10 * time.Second
	magicNumberDuration = 1 * time.Second
	headerDuration      = 2 * time.Second
	readDuration        = 5 * time.Second
	writeDuration       = 5 * time.Second
	keepAlivePeriod     = 10 * time.Second
	perConnBufSize      = uint64(1024)
	recvBufSize         = uint64(2048)

	plog = logger.GetLogger("transport")
)

const (
	// TransportName is the name of the tcp transport module.
	TransportName            = "go-quic-transport"
	requestHeaderSize        = 18
	raftType          uint16 = 100
	snapshotType      uint16 = 200
)

type quicTransport struct {
	nhConfig       config.NodeHostConfig
	stopper        *syncutil.Stopper
	connStopper    *syncutil.Stopper
	messageHandler raftio.MessageHandler
	chunkHandler   raftio.ChunkHandler
	encrypted      bool
}

func (q quicTransport) Name() string {
	return TransportName
}

func generateTLSConfig() *tls.Config {
	key, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		panic(err)
	}
	template := x509.Certificate{SerialNumber: big.NewInt(1)}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		panic(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		panic(err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		NextProtos:   []string{TransportName},
	}
}

// Start starts the QUIC transport module.
func (q *quicTransport) Start() error {
	address := q.nhConfig.GetListenAddress()
	//tlsConfig, err := q.nhConfig.GetServerTLSConfig()
	//if err != nil {
	//	return err
	//}
	q.stopper.RunWorker(func() {
		listener, err := quic.ListenAddr(address, generateTLSConfig(), nil)
		if err != nil {
			plog.Panicf("QUIC listener failed: %v", err)
		}
		defer func() {
			_ = listener.Close()
		}()

		for {
			s, err := listener.Accept(context.TODO())
			if err != nil {
				plog.Errorf("QUIC listener accept failed: %v", err)
				return
			}

			q.connStopper.RunWorker(func() {
				for {

					stream, err := s.AcceptStream(context.TODO())
					if err != nil {
						_ = s.CloseWithError(0, err.Error())
						return
					}

					go q.serveStream(stream)
				}
			})
		}
	})
	return nil
}

// Close stops the QUIC transport module.
func (q *quicTransport) Close() error {
	plog.Infof("Stopping transport")
	q.stopper.Stop()
	q.connStopper.Stop()
	plog.Infof("Transport stopped")
	return nil
}

func sendPoison(conn quic.Stream, poison []byte) error {
	tt := time.Now().Add(magicNumberDuration).Add(magicNumberDuration)
	if err := conn.SetWriteDeadline(tt); err != nil {
		return err
	}
	if _, err := conn.Write(poison); err != nil {
		return err
	}
	return nil
}

func sendPoisonAck(conn quic.Stream, poisonAck []byte) error {
	return sendPoison(conn, poisonAck)
}

func waitPoisonAck(conn quic.Stream) {
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

func (q *quicTransport) serveStream(stream quic.Stream) {
	magicNum := make([]byte, len(magicNumber))
	header := make([]byte, requestHeaderSize)
	tbuf := make([]byte, payloadBufferSize)
	for {
		err := readMagicNumber(stream, magicNum)
		if err != nil {
			if err == errPoisonReceived {
				_ = sendPoisonAck(stream, poisonNumber[:])
			}
			return
		}
		rheader, buf, err := readMessage(stream, header, tbuf, q.encrypted)
		if err != nil {
			return
		}
		if rheader.method == raftType {
			batch := raftpb.MessageBatch{}
			if err := batch.Unmarshal(buf); err != nil {
				return
			}
			q.messageHandler(batch)
		} else {
			chunk := raftpb.Chunk{}
			if err := chunk.Unmarshal(buf); err != nil {
				return
			}
			if !q.chunkHandler(chunk) {
				plog.Errorf("chunk rejected %d", chunk.ChunkId)
				return
			}
		}
	}
}

func readMessage(conn quic.Stream, header []byte, rbuf []byte, encrypted bool) (requestHeader, []byte, error) {
	tt := time.Now().Add(headerDuration)
	if err := conn.SetReadDeadline(tt); err != nil {
		return requestHeader{}, nil, err
	}
	if _, err := io.ReadFull(conn, header); err != nil {
		plog.Errorf("failed to get the requestHeader")
		return requestHeader{}, nil, err
	}
	rheader := requestHeader{}
	if !rheader.decode(header) {
		plog.Errorf("invalid requestHeader")
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

func readMagicNumber(conn quic.Stream, magicNum []byte) error {
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
