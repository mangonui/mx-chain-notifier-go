package process_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/multiversx/mx-chain-notifier-go/mocks"
	"github.com/multiversx/mx-chain-notifier-go/process"
	"github.com/stretchr/testify/require"
)

func TestTryCheckProcessedWithRetryReturnsFalseAfterRetryBudgetExhausted(t *testing.T) {
	restoreSleep := process.SetRetrySleepForTests(func(duration time.Duration) {})
	defer restoreSleep()

	callCount := 0
	args := createMockEventsHandlerArgs()
	args.Locker = &mocks.LockerStub{
		IsEventProcessedCalled: func(ctx context.Context, blockHash string) (bool, error) {
			callCount++
			return false, errors.New("still failing")
		},
		HasConnectionCalled: func(ctx context.Context) bool {
			return true
		},
	}

	eventsHandler, err := process.NewEventsHandler(args)
	require.NoError(t, err)

	ok := eventsHandler.TryCheckProcessedWithRetry("push", "hash")
	require.False(t, ok)
	require.Equal(t, 3, callCount)
}
