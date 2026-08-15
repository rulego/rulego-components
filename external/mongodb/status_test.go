package mongodb

import (
	"os"
	"testing"

	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/test/assert"
)

func TestClientNodeConnectionStatus(t *testing.T) {
	if os.Getenv("SKIP_MONGODB_TESTS") == "true" {
		t.Skip("Skipping MongoDB tests")
	}
	mongoURL := os.Getenv("MONGODB_URL")
	if mongoURL == "" {
		mongoURL = "mongodb://localhost:27017"
	}
	var node ClientNode
	config := types.NewConfig()
	err := node.Init(config, types.Configuration{
		"server":     mongoURL,
		"database":   "test",
		"collection": "user",
		"opType":     "QUERY",
		"filter":     `{"age": {"$gte": 18}}`,
	})
	assert.Nil(t, err)
	if _, err := node.SharedNode.GetSafely(); err != nil {
		t.Skipf("mongodb server not available: %v", err)
	}
	assert.Equal(t, types.StatusConnected, node.ConnectionStatus().Status)
	node.Destroy()
	assert.Equal(t, types.StatusDisconnected, node.ConnectionStatus().Status)
}
