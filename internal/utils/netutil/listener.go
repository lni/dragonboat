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

package netutil

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/lni/dragonboat/v3/internal/utils/stringutil"
)

var (
	// ErrListenerStopped indicates that the server has been stopped.
	ErrListenerStopped = errors.New("server stopped")
)

// StoppableListener is a type of TCP listener that can be stopped by
// signalling the associated stopc channel. It binds to all IPs resolved from
// the specified address.
type StoppableListener struct {
	listeners []net.Listener
	stopc     <-chan struct{}
	connc     chan net.Conn
	errc      chan error
	addr      string
	wg        sync.WaitGroup
}

func parseAddress(addr string) (string, string, error) {
	parts := strings.Split(addr, ":")
	if len(parts) == 2 {
		return parts[0], parts[1], nil
	}
	return "", "", errors.New("failed to get hostname")
}

func isListenerStopperError(err error) bool {
	// net/http/h2_bundle.go
	return strings.Contains(err.Error(), "use of closed network connection")
}

// NewStoppableListener returns a listener that can be stopped.
func NewStoppableListener(addr string, tlsConfig *tls.Config,
	stopc <-chan struct{}) (*StoppableListener, error) {
	addr = strings.TrimSpace(addr)
	hostname, port, err := parseAddress(addr)
	if err != nil {
		return nil, err
	}
	// workaround the design bug in golang's net package.
	// https://github.com/golang/go/issues/9334?ts=2
	listeners := make([]net.Listener, 0)
	toListen := make([]string, 0)
	if stringutil.HostnameRegex.MatchString(hostname) {
		ipList, err := net.LookupIP(hostname)
		if err != nil {
			return nil, err
		}
		for _, v := range ipList {
			// ipv6 address is ignored
			if v.To4() == nil {
				continue
			}
			toListen = append(toListen, fmt.Sprintf("%s:%s", v, port))
		}
	} else if stringutil.IPV4Regex.MatchString(hostname) {
		toListen = append(toListen, addr)
	}
	for _, v := range toListen {
		ln, err := net.Listen("tcp", v)
		if err != nil {
			return nil, err
		}
		listeners = append(listeners, ln)
	}
	s := &StoppableListener{
		listeners: listeners,
		stopc:     stopc,
		addr:      addr,
		errc:      make(chan error, len(listeners)),
		connc:     make(chan net.Conn, len(listeners)),
	}
	for _, lis := range s.listeners {
		gl := lis
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			for {
				tc, err := gl.Accept()
				if err != nil {
					select {
					case s.errc <- err:
					case <-s.stopc:
						return
					}
					if isListenerStopperError(err) {
						return
					}
					continue
				}
				tcpconn, ok := tc.(*net.TCPConn)
				if ok {
					if err := setTCPConn(tcpconn); err != nil {
						continue
					}
				}
				if tlsConfig != nil {
					tc = tls.Server(tc, tlsConfig)
					tt := time.Now().Add(3 * time.Second)
					if err := tc.SetDeadline(tt); err != nil {
						continue
					}
					if err := tc.(*tls.Conn).Handshake(); err != nil {
						continue
					}
				}
				select {
				case s.connc <- tc:
				case <-s.stopc:
					return
				}
			}
		}()
	}
	return s, nil
}

func setTCPConn(conn *net.TCPConn) error {
	if err := conn.SetLinger(0); err != nil {
		return err
	}
	if err := conn.SetKeepAlive(true); err != nil {
		return err
	}
	return conn.SetKeepAlivePeriod(20 * time.Second)
}

// Accept starts to accept incoming connections.
func (ln *StoppableListener) Accept() (net.Conn, error) {
	select {
	case <-ln.stopc:
		// see https://github.com/golang/go/issues/10527
		var err error
		for _, v := range ln.listeners {
			if e := v.Close(); e != nil {
				err = e
			}
		}
		ln.wg.Wait()
		if err == nil {
			err = ErrListenerStopped
		}
		return nil, err
	case err := <-ln.errc:
		return nil, err
	case c := <-ln.connc:
		return c, nil
	}
}

// Close closes the listener.
func (ln *StoppableListener) Close() error {
	for _, v := range ln.listeners {
		if err := v.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Addr returns the net.Addr of the listener.
func (ln *StoppableListener) Addr() net.Addr {
	panic("Addr not implemented")
}
