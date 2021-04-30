package quic

import (
	"context"
	"crypto/tls"

	"github.com/lni/dragonboat/v3/raftio"
	"github.com/lucas-clemente/quic-go"
)

func (q *quicTransport) GetConnection(ctx context.Context, target string) (raftio.IConnection, error) {
	str, err := q.openStreamTo(ctx, target)
	if err != nil {
		return nil, err
	}
	return NewMessageConnection(str, q.encrypted), nil
}

func (q *quicTransport) GetSnapshotConnection(ctx context.Context, target string) (raftio.ISnapshotConnection, error) {
	str, err := q.openStreamTo(ctx, target)
	if err != nil {
		return nil, err
	}
	return NewSnapshotConnection(str, q.encrypted), nil
}

func (q *quicTransport) openStreamTo(ctx context.Context, target string) (quic.Stream, error) {
	tlsConf := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{TransportName},
	}
	session, err := quic.DialAddrContext(ctx, target, tlsConf, nil)
	if err != nil {
		return nil, err
	}
	str, err := session.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	return str, nil
}
