package server

import (
	"git.apache.org/thrift.git/lib/go/thrift"
)

// Connect connects to the specified server
func Connect() (*ServerClient, *thrift.TBufferedTransport, error) {
	transport, err := thrift.NewTSocket("127.0.0.1:9090")
	if err != nil {
		return nil, nil, err
	}
	bufferedTrans := thrift.NewTBufferedTransport(transport, 1024*16)
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	client := NewServerClientFactory(bufferedTrans, protocolFactory)
	bufferedTrans.Open()
	return client, bufferedTrans, nil
}
