package ws

import (
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/gorilla/websocket"
	"github.com/multiversx/mx-chain-notifier-go/dispatcher"
)

type wsUpgraderWrapper struct {
	upgrader *websocket.Upgrader
}

// NewWSUpgraderWrapper creates a websocket upgrader wrapper
func NewWSUpgraderWrapper(readBuffSize int, writeBuffSize int, allowEmptyOrigin bool) (dispatcher.WSUpgrader, error) {
	if readBuffSize <= 0 {
		return nil, fmt.Errorf("invalid buffer size provided: %d", readBuffSize)
	}
	if writeBuffSize <= 0 {
		return nil, fmt.Errorf("invalid buffer size provided: %d", writeBuffSize)
	}

	upgrader := &websocket.Upgrader{
		ReadBufferSize:   readBuffSize,
		WriteBufferSize:  writeBuffSize,
		HandshakeTimeout: 10 * time.Second,
		CheckOrigin:      checkOriginFunc(allowEmptyOrigin),
	}

	return &wsUpgraderWrapper{
		upgrader: upgrader,
	}, nil
}

// Upgrade upgrades the HTTP server connection to the websocket protocol
func (wuw *wsUpgraderWrapper) Upgrade(w http.ResponseWriter, r *http.Request, responseHeader http.Header) (dispatcher.WSConnection, error) {
	return wuw.upgrader.Upgrade(w, r, responseHeader)
}

func checkOriginFunc(allowEmptyOrigin bool) func(r *http.Request) bool {
	return func(r *http.Request) bool {
		return checkOrigin(r, allowEmptyOrigin)
	}
}

func checkOrigin(r *http.Request, allowEmptyOrigin bool) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return allowEmptyOrigin
	}

	originURL, err := url.Parse(origin)
	if err != nil {
		return false
	}

	return originURL.Host == r.Host
}
