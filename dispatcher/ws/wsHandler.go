package ws

import (
	"net/http"
	"sync/atomic"

	"github.com/multiversx/mx-chain-core-go/core/check"
	"github.com/multiversx/mx-chain-core-go/marshal"
	"github.com/multiversx/mx-chain-notifier-go/common"
	"github.com/multiversx/mx-chain-notifier-go/dispatcher"
)

const defaultMaxConnections = 1024

// ArgsWebSocketProcessor defines the argument needed to create a websocketHandler.
// MaxConnections <= 0 means "use the default cap (1024)".
type ArgsWebSocketProcessor struct {
	Dispatcher     dispatcher.Dispatcher
	Upgrader       dispatcher.WSUpgrader
	Marshaller     marshal.Marshalizer
	MaxConnections int64
}

type websocketProcessor struct {
	dispatcher     dispatcher.Dispatcher
	upgrader       dispatcher.WSUpgrader
	marshaller     marshal.Marshalizer
	maxConnections int64
	connCount      atomic.Int64
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
	if !wh.tryReserveConnection() {
		http.Error(w, "too many websocket connections", http.StatusServiceUnavailable)
		return
	}

	conn, err := wh.upgrader.Upgrade(w, r, nil)
	if err != nil {
		wh.releaseConnection()
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
		wh.releaseConnection()
		_ = conn.Close()
		log.Error("failed creating a new websocket dispatcher", "err", err.Error())
		return
	}
	wsDispatcher.dispatcher.RegisterEvent(wsDispatcher)

	go func() {
		defer wh.releaseConnection()
		wsDispatcher.writePump()
	}()
	go wsDispatcher.readPump()
}

func (wh *websocketProcessor) tryReserveConnection() bool {
	for {
		current := wh.connCount.Load()
		if current >= wh.maxConnections {
			return false
		}
		if wh.connCount.CompareAndSwap(current, current+1) {
			return true
		}
	}
}

func (wh *websocketProcessor) releaseConnection() {
	wh.connCount.Add(-1)
}

// IsInterfaceNil returns true if there is no value under the interface
func (wh *websocketProcessor) IsInterfaceNil() bool {
	return wh == nil
}
