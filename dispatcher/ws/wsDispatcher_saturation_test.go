package ws_test

import (
	"testing"
	"time"

	"github.com/multiversx/mx-chain-notifier-go/data"
	"github.com/multiversx/mx-chain-notifier-go/dispatcher/ws"
	"github.com/stretchr/testify/require"
)

func TestPushEventsDoesNotBlockWhenQueueIsFull(t *testing.T) {
	args := createMockWSDispatcherArgs()
	wd, err := ws.NewTestWSDispatcher(args)
	require.NoError(t, err)

	events := []data.Event{{
		Address: "addr1",
	}}

	for i := 0; i < wd.SendQueueCap(); i++ {
		wd.PushEvents(events)
	}
	require.Equal(t, wd.SendQueueCap(), wd.SendQueueLen())

	done := make(chan struct{})
	go func() {
		wd.PushEvents(events)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("PushEvents blocked on full websocket queue")
	}

	require.Equal(t, wd.SendQueueCap(), wd.SendQueueLen())
}
