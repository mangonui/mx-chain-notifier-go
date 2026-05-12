package ws

import (
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/multiversx/mx-chain-core-go/core/check"
	"github.com/multiversx/mx-chain-core-go/marshal"
	"github.com/multiversx/mx-chain-notifier-go/common"
	"github.com/multiversx/mx-chain-notifier-go/dispatcher"
)

const (
	defaultMaxConnections      = 1024
	defaultMaxConnectionsPerIP = 64
)

// ArgsWebSocketProcessor defines the argument needed to create a websocketHandler.
// MaxConnections <= 0 means "use the default cap (1024)".
type ArgsWebSocketProcessor struct {
	Dispatcher     dispatcher.Dispatcher
	Upgrader       dispatcher.WSUpgrader
	Marshaller     marshal.Marshalizer
	MaxConnections int64
}

type websocketProcessor struct {
	dispatcher       dispatcher.Dispatcher
	upgrader         dispatcher.WSUpgrader
	marshaller       marshal.Marshalizer
	maxConnections   int64
	connCount        atomic.Int64
	ipConnectionsMut sync.Mutex
	ipConnections    map[string]int64
}

// NewWebSocketProcessor creates a new websocketProcessor component
func NewWebSocketProcessor(args ArgsWebSocketProcessor) (*websocketProcessor, error) {
	err := checkArgs(args)
	if err != nil {
		return nil, err
	}

	maxConn := args.MaxConnections
	if maxConn <= 0 {
		maxConn = defaultMaxConnections
	}

	return &websocketProcessor{
		dispatcher:     args.Dispatcher,
		upgrader:       args.Upgrader,
		marshaller:     args.Marshaller,
		maxConnections: maxConn,
		ipConnections:  make(map[string]int64),
	}, nil
}

func checkArgs(args ArgsWebSocketProcessor) error {
	if check.IfNil(args.Dispatcher) {
		return ErrNilDispatcher
	}
	if args.Upgrader == nil {
		return ErrNilWSUpgrader
	}
	if check.IfNil(args.Marshaller) {
		return common.ErrNilMarshaller
	}

	return nil
}

// ServeHTTP is the entry point used by a http server to serve the websocket upgrader
func (wh *websocketProcessor) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	remoteIP := remoteIPFromRequest(r)
	if !wh.tryReserveConnection(remoteIP) {
		http.Error(w, "too many websocket connections", http.StatusServiceUnavailable)
		return
	}

	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { wh.releaseConnection(remoteIP) }) }

	conn, err := wh.upgrader.Upgrade(w, r, nil)
	if err != nil {
		release()
		log.Error("failed upgrading connection", "err", err.Error())
		return
	}

	args := argsWebSocketDispatcher{
		Dispatcher: wh.dispatcher,
		Conn:       conn,
		Marshaller: wh.marshaller,
	}
	wsDispatcher, err := newWebSocketDispatcher(args)
	if err != nil {
		release()
		_ = conn.Close()
		log.Error("failed creating a new websocket dispatcher", "err", err.Error())
		return
	}
	wsDispatcher.dispatcher.RegisterEvent(wsDispatcher)

	go runPump("writePump", release, wsDispatcher.writePump)
	go runPump("readPump", release, wsDispatcher.readPump)
}

// runPump executes a websocket pump under a panic guard and guarantees the
// reservation release runs exactly once across both pumps (caller wraps
// release with sync.Once). A panic inside the pump is logged with the pump
// name and recovered so it does not crash the process.
func runPump(name string, release func(), pump func()) {
	defer release()
	defer func() {
		if r := recover(); r != nil {
			log.Error("panic in websocket pump", "pump", name, "panic", fmt.Sprintf("%v", r))
		}
	}()
	pump()
}

func (wh *websocketProcessor) tryReserveConnection(remoteIP string) bool {
	if !wh.tryReserveIPConnection(remoteIP) {
		return false
	}

	for {
		current := wh.connCount.Load()
		if current >= wh.maxConnections {
			wh.releaseIPConnection(remoteIP)
			return false
		}
		if wh.connCount.CompareAndSwap(current, current+1) {
			return true
		}
	}
}

func (wh *websocketProcessor) releaseConnection(remoteIP string) {
	wh.connCount.Add(-1)
	wh.releaseIPConnection(remoteIP)
}

func (wh *websocketProcessor) tryReserveIPConnection(remoteIP string) bool {
	wh.ipConnectionsMut.Lock()
	defer wh.ipConnectionsMut.Unlock()

	current := wh.ipConnections[remoteIP]
	if current >= defaultMaxConnectionsPerIP {
		return false
	}

	wh.ipConnections[remoteIP] = current + 1
	return true
}

func (wh *websocketProcessor) releaseIPConnection(remoteIP string) {
	wh.ipConnectionsMut.Lock()
	defer wh.ipConnectionsMut.Unlock()

	current := wh.ipConnections[remoteIP]
	if current <= 1 {
		delete(wh.ipConnections, remoteIP)
		return
	}

	wh.ipConnections[remoteIP] = current - 1
}

func remoteIPFromRequest(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil && host != "" {
		return host
	}
	if r.RemoteAddr != "" {
		return r.RemoteAddr
	}

	return "unknown"
}

// IsInterfaceNil returns true if there is no value under the interface
func (wh *websocketProcessor) IsInterfaceNil() bool {
	return wh == nil
}
