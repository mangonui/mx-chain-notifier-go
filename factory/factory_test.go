package factory

import (
	"testing"

	"github.com/multiversx/mx-chain-core-go/marshal"
	"github.com/multiversx/mx-chain-notifier-go/common"
	"github.com/multiversx/mx-chain-notifier-go/config"
	"github.com/stretchr/testify/require"
)

func TestCreateHub(t *testing.T) {
	t.Run("websocket publisher", func(t *testing.T) {
		hub, err := CreateHub(common.WSPublisherType)

		require.Nil(t, err)
		require.NotNil(t, hub)
		require.False(t, hub.IsInterfaceNil())
	})

	t.Run("message queue publisher returns disabled hub", func(t *testing.T) {
		hub, err := CreateHub(common.MessageQueuePublisherType)

		require.Nil(t, err)
		require.NotNil(t, hub)
		require.False(t, hub.IsInterfaceNil())
	})

	t.Run("invalid API type", func(t *testing.T) {
		hub, err := CreateHub("invalid")

		require.Nil(t, hub)
		require.Equal(t, common.ErrInvalidAPIType, err)
	})
}

func TestCreateLockService(t *testing.T) {
	t.Run("duplicates disabled", func(t *testing.T) {
		lockService, err := CreateLockService(false, config.RedisConfig{})

		require.Nil(t, err)
		require.NotNil(t, lockService)
		require.False(t, lockService.IsInterfaceNil())
	})

	t.Run("invalid redis connection type", func(t *testing.T) {
		lockService, err := CreateLockService(true, config.RedisConfig{ConnectionType: "invalid"})

		require.Nil(t, lockService)
		require.Equal(t, common.ErrInvalidRedisConnType, err)
	})
}

func TestCreateEventsInterceptorShouldRejectInvalidPubkeyConverter(t *testing.T) {
	interceptor, err := CreateEventsInterceptor(config.GeneralConfig{
		AddressConverter: config.AddressConverterConfig{Type: "invalid"},
	})

	require.Nil(t, interceptor)
	require.Equal(t, common.ErrInvalidPubKeyConverterType, err)
}

func TestCreateWSHandlerShouldRejectInvalidAPIType(t *testing.T) {
	handler, err := CreateWSHandler("invalid", nil, &marshal.JsonMarshalizer{}, config.ConnectorApiConfig{})

	require.Nil(t, handler)
	require.Equal(t, common.ErrInvalidAPIType, err)
}
