package factory

import (
	"net"
	"strings"

	"github.com/multiversx/mx-chain-communication-go/websocket/data"
	factoryHost "github.com/multiversx/mx-chain-communication-go/websocket/factory"
	"github.com/multiversx/mx-chain-core-go/marshal"
	marshalFactory "github.com/multiversx/mx-chain-core-go/marshal/factory"
	"github.com/multiversx/mx-chain-notifier-go/common"
	"github.com/multiversx/mx-chain-notifier-go/config"
	"github.com/multiversx/mx-chain-notifier-go/disabled"
	"github.com/multiversx/mx-chain-notifier-go/dispatcher"
	"github.com/multiversx/mx-chain-notifier-go/dispatcher/ws"
	"github.com/multiversx/mx-chain-notifier-go/process"
)

const (
	readBufferSize  = 1024
	writeBufferSize = 1024
)

// CreateWSHandler creates websocket handler component based on api type.
// maxConnections caps concurrent /hub/ws connections (<=0 means use the default).
func CreateWSHandler(apiType string, wsDispatcher dispatcher.Dispatcher, marshaller marshal.Marshalizer, connectorAPIConfig config.ConnectorApiConfig) (dispatcher.WSHandler, error) {
	switch apiType {
	case common.MessageQueuePublisherType:
		return &disabled.WSHandler{}, nil
	case common.WSPublisherType:
		return createWSHandler(wsDispatcher, marshaller, connectorAPIConfig)
	default:
		return nil, common.ErrInvalidAPIType
	}
}

func createWSHandler(wsDispatcher dispatcher.Dispatcher, marshaller marshal.Marshalizer, connectorAPIConfig config.ConnectorApiConfig) (dispatcher.WSHandler, error) {
	// ISSUE-028: AllowEmptyOrigin=true disables browser CSRF protection on
	// the WS upgrade (non-browser clients have no Origin header). That is
	// legitimate for server-to-server use over loopback, but with a public
	// bind it removes the only browser-side defense. Log a loud warning
	// so this combination is auditable from a single grep of startup logs.
	if connectorAPIConfig.AllowEmptyOrigin && !isLoopbackHost(connectorAPIConfig.Host) {
		log.Warn("notifier WS AllowEmptyOrigin=true with non-loopback Host — "+
			"this disables browser CSRF protection on WebSocket upgrades; "+
			"front the notifier with an auth-aware reverse proxy or restrict the bind",
			"host", connectorAPIConfig.Host)
	}

	upgrader, err := ws.NewWSUpgraderWrapper(readBufferSize, writeBufferSize, connectorAPIConfig.AllowEmptyOrigin)
	if err != nil {
		return nil, err
	}

	args := ws.ArgsWebSocketProcessor{
		Dispatcher:               wsDispatcher,
		Upgrader:                 upgrader,
		Marshaller:               marshaller,
		MaxConnections:           connectorAPIConfig.MaxConnections,
		MaxConnectionRatePerIP:   connectorAPIConfig.MaxConnectionRatePerIP,
		ConnectionRateBurstPerIP: connectorAPIConfig.ConnectionRateBurstPerIP,
	}
	return ws.NewWebSocketProcessor(args)
}

// CreateWSObserverConnector will create the web socket connector for observer node communication
func CreateWSObserverConnector(
	config config.WebSocketConfig,
	facade process.EventsFacadeHandler,
) (process.WSClient, error) {
	if config.Enabled {
		return createWsObsConnector(config, facade)
	}

	return &disabled.WSHandler{}, nil
}

func createWsObsConnector(
	config config.WebSocketConfig,
	facade process.EventsFacadeHandler,
) (process.WSClient, error) {
	marshaller, err := marshalFactory.NewMarshalizer(config.DataMarshallerType)
	if err != nil {
		return nil, err
	}

	host, err := createWsHost(config, marshaller)
	if err != nil {
		return nil, err
	}

	payloadHandler, err := CreatePayloadHandler(marshaller, facade)
	if err != nil {
		return nil, err
	}

	err = host.SetPayloadHandler(payloadHandler)
	if err != nil {
		return nil, err
	}

	return host, nil
}

// isLoopbackHost reports whether the given Host field (as configured in
// notifier.toml under [ConnectorApi]) binds to a loopback address. An
// empty Host means "default" which the webServer layer treats as
// localhost:5000 — therefore loopback. Bare ports like ":5000" bind to
// all interfaces and are NOT loopback. See issues/ISSUE-028.
func isLoopbackHost(host string) bool {
	if host == "" {
		return true
	}
	hostname, _, err := net.SplitHostPort(host)
	if err != nil {
		// Not a host:port form; treat the whole value as the hostname.
		hostname = host
	}
	if hostname == "" {
		// Bare ":port" form binds to all interfaces.
		return false
	}
	hostname = strings.ToLower(hostname)
	if hostname == "localhost" {
		return true
	}
	ip := net.ParseIP(hostname)
	if ip == nil {
		return false
	}
	return ip.IsLoopback()
}

func createWsHost(wsConfig config.WebSocketConfig, wsMarshaller marshal.Marshalizer) (factoryHost.FullDuplexHost, error) {
	return factoryHost.CreateWebSocketHost(factoryHost.ArgsWebSocketHost{
		WebSocketConfig: data.WebSocketConfig{
			URL:                        wsConfig.URL,
			WithAcknowledge:            wsConfig.WithAcknowledge,
			Mode:                       wsConfig.Mode,
			RetryDurationInSec:         int(wsConfig.RetryDurationInSec),
			BlockingAckOnError:         wsConfig.BlockingAckOnError,
			AcknowledgeTimeoutInSec:    wsConfig.AcknowledgeTimeoutInSec,
			DropMessagesIfNoConnection: wsConfig.DropMessagesIfNoConnection,
		},
		Marshaller: wsMarshaller,
		Log:        log,
	})
}
