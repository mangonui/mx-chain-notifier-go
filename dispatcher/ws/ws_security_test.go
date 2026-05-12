package ws

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/multiversx/mx-chain-core-go/core/mock"
	"github.com/multiversx/mx-chain-notifier-go/dispatcher"
	"github.com/multiversx/mx-chain-notifier-go/mocks"
	"github.com/stretchr/testify/require"
)

func TestCheckOrigin(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest(http.MethodGet, "http://notifier.local/hub/ws", nil)
	require.True(t, checkOrigin(req))

	req.Header.Set("Origin", "http://notifier.local")
	require.True(t, checkOrigin(req))

	req.Header.Set("Origin", "https://evil.example")
	require.False(t, checkOrigin(req))

	req.Header.Set("Origin", "://bad-origin")
	require.False(t, checkOrigin(req))
}

func TestWebSocketProcessor_RejectsConnectionsAboveLimit(t *testing.T) {
	t.Parallel()

	const testCap = int64(3)
	args := ArgsWebSocketProcessor{
		Dispatcher: &mocks.HubStub{},
		Upgrader: &mocks.WSUpgraderStub{
			UpgradeCalled: func(w http.ResponseWriter, r *http.Request, responseHeader http.Header) (dispatcher.WSConnection, error) {
				require.Fail(t, "upgrade should not be called when connection limit is reached")
				return nil, nil
			},
		},
		Marshaller:     &mock.MarshalizerMock{},
		MaxConnections: testCap,
	}
	processor, err := NewWebSocketProcessor(args)
	require.NoError(t, err)
	processor.connCount.Store(testCap)

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/hub/ws", nil)
	processor.ServeHTTP(response, request)

	require.Equal(t, http.StatusServiceUnavailable, response.Code)
	require.Equal(t, testCap, processor.connCount.Load())
}

func TestWebSocketProcessor_DefaultsMaxConnectionsWhenZero(t *testing.T) {
	t.Parallel()

	args := ArgsWebSocketProcessor{
		Dispatcher: &mocks.HubStub{},
		Upgrader:   &mocks.WSUpgraderStub{},
		Marshaller: &mock.MarshalizerMock{},
	}
	processor, err := NewWebSocketProcessor(args)
	require.NoError(t, err)
	require.Equal(t, int64(defaultMaxConnections), processor.maxConnections)
}

func TestWebSocketProcessor_ReleasesReservationOnUpgradeError(t *testing.T) {
	t.Parallel()

	args := ArgsWebSocketProcessor{
		Dispatcher: &mocks.HubStub{},
		Upgrader: &mocks.WSUpgraderStub{
			UpgradeCalled: func(w http.ResponseWriter, r *http.Request, responseHeader http.Header) (dispatcher.WSConnection, error) {
				return nil, errors.New("upgrade failed")
			},
		},
		Marshaller: &mock.MarshalizerMock{},
	}
	processor, err := NewWebSocketProcessor(args)
	require.NoError(t, err)

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/hub/ws", nil)
	processor.ServeHTTP(response, request)

	require.Zero(t, processor.connCount.Load())
}
