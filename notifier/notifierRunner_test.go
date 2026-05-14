package notifier

import (
	"testing"

	"github.com/multiversx/mx-chain-notifier-go/config"
	"github.com/stretchr/testify/require"
)

func TestNewNotifierRunner(t *testing.T) {
	t.Run("nil config", func(t *testing.T) {
		runner, err := NewNotifierRunner(nil)

		require.Nil(t, runner)
		require.Equal(t, ErrNilConfigs, err)
	})

	t.Run("should work", func(t *testing.T) {
		cfgs := &config.Configs{}
		runner, err := NewNotifierRunner(cfgs)

		require.Nil(t, err)
		require.NotNil(t, runner)
		require.Equal(t, *cfgs, runner.configs)
	})
}
