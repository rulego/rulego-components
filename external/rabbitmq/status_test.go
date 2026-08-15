package rabbitmq

import (
	"os"
	"testing"

	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/test/assert"
)

func TestClientNodeConnectionStatus(t *testing.T) {
	if os.Getenv("SKIP_RABBITMQ_TESTS") == "true" {
		t.Skip("Skipping RabbitMQ tests")
	}
	rabbitmqURL := os.Getenv("RABBITMQ_URL")
	if rabbitmqURL == "" {
		rabbitmqURL = "amqp://guest:guest@localhost:5672/"
	}
	var node ClientNode
	config := types.NewConfig()
	err := node.Init(config, types.Configuration{
		"server":   rabbitmqURL,
		"exchange": "test_exchange",
		"key":      "test.route",
	})
	assert.Nil(t, err)
	conn, err := node.SharedNode.GetSafely()
	if err != nil {
		t.Skipf("rabbitmq server not available: %v", err)
	}
	assert.Equal(t, types.StatusConnected, node.ConnectionStatus().Status)

	// 服务端/网络侧断连时 IsClosed 立即可见，无需等下次使用
	err = conn.Close()
	assert.Nil(t, err)
	info := node.ConnectionStatus()
	assert.Equal(t, types.StatusReconnecting, info.Status)

	node.Destroy()
	assert.Equal(t, types.StatusDisconnected, node.ConnectionStatus().Status)
}
