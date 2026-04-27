package gin

import (
	"net/http"
	"testing"
	"time"

	"github.com/multiversx/mx-chain-communication-go/testscommon"
	"github.com/multiversx/mx-chain-notifier-go/config"
	"github.com/multiversx/mx-chain-notifier-go/mocks"
	"github.com/stretchr/testify/require"
)

func TestRunConfiguresReadHeaderTimeout(t *testing.T) {
	args := ArgsWebServerHandler{
		Facade:         &mocks.FacadeStub{},
		PayloadHandler: &testscommon.PayloadHandlerStub{},
		Configs: config.Configs{
			MainConfig: config.MainConfig{
				ConnectorApi: config.ConnectorApiConfig{
					Host: "127.0.0.1:0",
				},
			},
			Flags: config.FlagsConfig{
				PublisherType: "notifier",
			},
		},
	}

	ws, err := NewWebServerHandler(args)
	require.NoError(t, err)

	err = ws.Run()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = ws.Close()
	})

	internal, ok := ws.httpServer.(*httpServerWrapper)
	require.True(t, ok)
	server, ok := internal.server.(*http.Server)
	require.True(t, ok)
	require.Equal(t, 5*time.Second, server.ReadHeaderTimeout)
}
