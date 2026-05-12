package ws

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/multiversx/mx-chain-core-go/core/mock"
	"github.com/multiversx/mx-chain-notifier-go/dispatcher"
	"github.com/multiversx/mx-chain-notifier-go/mocks"
	"github.com/stretchr/testify/require"
)

func TestCheckOrigin(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest(http.MethodGet, "http://notifier.local/hub/ws", nil)
	require.False(t, checkOrigin(req, false))
	require.True(t, checkOrigin(req, true))

	req.Header.Set("Origin", "http://notifier.local")
	require.True(t, checkOrigin(req, false))

	req.Header.Set("Origin", "https://evil.example")
	require.False(t, checkOrigin(req, true))

	req.Header.Set("Origin", "://bad-origin")
	require.False(t, checkOrigin(req, true))
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

func TestRunPump_NormalExitCallsRelease(t *testing.T) {
	t.Parallel()

	released := 0
	runPump("test", func() { released++ }, func() {})
	require.Equal(t, 1, released)
}

func TestRunPump_PanicIsRecoveredAndReleaseStillRuns(t *testing.T) {
	t.Parallel()

	released := 0
	require.NotPanics(t, func() {
		runPump("test", func() { released++ }, func() { panic("boom") })
	})
	require.Equal(t, 1, released)
}

func TestWebSocketProcessor_ReleaseIsOnce(t *testing.T) {
	t.Parallel()

	args := ArgsWebSocketProcessor{
		Dispatcher:     &mocks.HubStub{},
		Upgrader:       &mocks.WSUpgraderStub{},
		Marshaller:     &mock.MarshalizerMock{},
		MaxConnections: 4,
	}
	processor, err := NewWebSocketProcessor(args)
	require.NoError(t, err)
	processor.connCount.Store(1)

	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { processor.releaseConnection("127.0.0.1") }) }

	release()
	release()
	release()

	require.Zero(t, processor.connCount.Load(),
		"sync.Once must guarantee the per-connection release decrements exactly once")
}

func TestWebSocketProcessor_RejectsConnectionsAbovePerIPLimit(t *testing.T) {
	t.Parallel()

	args := ArgsWebSocketProcessor{
		Dispatcher: &mocks.HubStub{},
		Upgrader: &mocks.WSUpgraderStub{
			UpgradeCalled: func(w http.ResponseWriter, r *http.Request, responseHeader http.Header) (dispatcher.WSConnection, error) {
				require.Fail(t, "upgrade should not be called when per-IP limit is reached")
				return nil, nil
			},
		},
		Marshaller:     &mock.MarshalizerMock{},
		MaxConnections: defaultMaxConnections,
	}
	processor, err := NewWebSocketProcessor(args)
	require.NoError(t, err)

	processor.ipConnectionsMut.Lock()
	processor.ipConnections["192.0.2.10"] = defaultMaxConnectionsPerIP
	processor.ipConnectionsMut.Unlock()

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/hub/ws", nil)
	request.RemoteAddr = "192.0.2.10:1234"
	processor.ServeHTTP(response, request)

	require.Equal(t, http.StatusServiceUnavailable, response.Code)
	require.Zero(t, processor.connCount.Load())
	require.Equal(t, int64(defaultMaxConnectionsPerIP), processor.ipConnections["192.0.2.10"])
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
	require.Empty(t, processor.ipConnections)
}

func TestWebSocketProcessor_PerIPCounterCleanupDoesNotLoseActiveReservations(t *testing.T) {
	t.Parallel()

	args := ArgsWebSocketProcessor{
		Dispatcher:     &mocks.HubStub{},
		Upgrader:       &mocks.WSUpgraderStub{},
		Marshaller:     &mock.MarshalizerMock{},
		MaxConnections: defaultMaxConnections,
	}
	processor, err := NewWebSocketProcessor(args)
	require.NoError(t, err)

	require.Equal(t, reservationOK, processor.tryReserveConnection("192.0.2.20"))
	require.Equal(t, reservationOK, processor.tryReserveConnection("192.0.2.20"))

	processor.releaseConnection("192.0.2.20")
	require.Equal(t, int64(1), processor.ipConnections["192.0.2.20"])
	require.Equal(t, int64(1), processor.connCount.Load())

	processor.releaseConnection("192.0.2.20")
	require.NotContains(t, processor.ipConnections, "192.0.2.20")
	require.Zero(t, processor.connCount.Load())
}

func TestWebSocketProcessor_RateLimitsConnectionAttemptsPerIP(t *testing.T) {
	t.Parallel()

	args := ArgsWebSocketProcessor{
		Dispatcher:               &mocks.HubStub{},
		Upgrader:                 &mocks.WSUpgraderStub{},
		Marshaller:               &mock.MarshalizerMock{},
		MaxConnections:           defaultMaxConnections,
		MaxConnectionRatePerIP:   1,
		ConnectionRateBurstPerIP: 1,
	}
	processor, err := NewWebSocketProcessor(args)
	require.NoError(t, err)

	require.Equal(t, reservationOK, processor.tryReserveConnection("192.0.2.30"))
	processor.releaseConnection("192.0.2.30")
	require.Equal(t, reservationRateLimited, processor.tryReserveConnection("192.0.2.30"))
	require.Zero(t, processor.connCount.Load())
}

func TestWebSocketProcessor_RateLimiterRefillsAndPrunes(t *testing.T) {
	t.Parallel()

	args := ArgsWebSocketProcessor{
		Dispatcher:               &mocks.HubStub{},
		Upgrader:                 &mocks.WSUpgraderStub{},
		Marshaller:               &mock.MarshalizerMock{},
		MaxConnectionRatePerIP:   2,
		ConnectionRateBurstPerIP: 3,
	}
	processor, err := NewWebSocketProcessor(args)
	require.NoError(t, err)

	now := time.Unix(100, 0)
	require.True(t, processor.allowConnectionAttempt("192.0.2.40", now))
	require.True(t, processor.allowConnectionAttempt("192.0.2.40", now))
	require.True(t, processor.allowConnectionAttempt("192.0.2.40", now))
	require.False(t, processor.allowConnectionAttempt("192.0.2.40", now))
	require.True(t, processor.allowConnectionAttempt("192.0.2.40", now.Add(time.Second)))

	processor.rateLimitMut.Lock()
	processor.rateLimiters["192.0.2.41"] = &ipRateLimiter{lastSeen: now.Add(-rateLimiterMaxIdleDuration - time.Second)}
	processor.pruneIdleRateLimiters(now)
	_, exists := processor.rateLimiters["192.0.2.41"]
	processor.rateLimitMut.Unlock()
	require.False(t, exists)
}
