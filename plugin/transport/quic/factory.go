package quic

import (
	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/raftio"
	"github.com/lni/goutils/stringutil"
	"github.com/lni/goutils/syncutil"
)

type TransportFactory struct {
}

func (t *TransportFactory) Create(config config.NodeHostConfig, mHandler raftio.MessageHandler, chHandler raftio.ChunkHandler) raftio.ITransport {
	return &quicTransport{
		nhConfig:       config,
		messageHandler: mHandler,
		chunkHandler:   chHandler,
		stopper:        syncutil.NewStopper(),
		connStopper:    syncutil.NewStopper(),
		encrypted:      config.MutualTLS,
	}
}

func (t *TransportFactory) Validate(addr string) bool {
	return stringutil.IsValidAddress(addr)
}
