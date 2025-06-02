package opengemini

import (
	"github.com/openGemini/opengemini-client-go/opengemini"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/test"
	"github.com/rulego/rulego/test/assert"
	"strings"
	"testing"
	"time"
)

func TestQueryNode(t *testing.T) {
	Registry := &types.SafeComponentSlice{}
	Registry.Add(&WriteNode{})
	Registry.Add(&QueryNode{})
	var queryNodeType = "x/opengeminiQuery"
	//var writeNodeType = "x/opengeminiWrite"

	t.Run("NewNode", func(t *testing.T) {
		test.NodeNew(t, queryNodeType, &QueryNode{}, types.Configuration{
			"server":   "127.0.0.1:8086",
			"database": "db0",
			"command":  "select * from cpu_load",
		}, Registry)
	})

	t.Run("InitNode", func(t *testing.T) {
		node, _ := test.CreateAndInitNode(queryNodeType, types.Configuration{
			"server":   "127.0.0.1:8086,127.0.0.1:8087",
			"database": "db0",
			"token":    "aaa",
		}, Registry)
		assert.Equal(t, "aaa", node.(*QueryNode).opengeminiConfig.AuthConfig.Token)
		assert.Equal(t, "127.0.0.1", node.(*QueryNode).opengeminiConfig.Addresses[0].Host)
		assert.Equal(t, 8086, node.(*QueryNode).opengeminiConfig.Addresses[0].Port)
		assert.Equal(t, "127.0.0.1", node.(*QueryNode).opengeminiConfig.Addresses[1].Host)
		assert.Equal(t, 8087, node.(*QueryNode).opengeminiConfig.Addresses[1].Port)
		assert.Equal(t, opengemini.AuthTypeToken, node.(*QueryNode).opengeminiConfig.AuthConfig.AuthType)

		node, _ = test.CreateAndInitNode(queryNodeType, types.Configuration{
			"server":   "127.0.0.1:8086,192.168.0.1:8087",
			"database": "db0",
			"username": "aaa",
			"password": "bbb",
			"command":  "select * from cpu_load",
		}, Registry)
		assert.Equal(t, "", node.(*QueryNode).opengeminiConfig.AuthConfig.Token)
		assert.Equal(t, "127.0.0.1", node.(*QueryNode).opengeminiConfig.Addresses[0].Host)
		assert.Equal(t, 8086, node.(*QueryNode).opengeminiConfig.Addresses[0].Port)
		assert.Equal(t, "192.168.0.1", node.(*QueryNode).opengeminiConfig.Addresses[1].Host)
		assert.Equal(t, 8087, node.(*QueryNode).opengeminiConfig.Addresses[1].Port)
		assert.Equal(t, opengemini.AuthTypePassword, node.(*QueryNode).opengeminiConfig.AuthConfig.AuthType)
		assert.Equal(t, "aaa", node.(*QueryNode).opengeminiConfig.AuthConfig.Username)
		assert.Equal(t, "bbb", node.(*QueryNode).opengeminiConfig.AuthConfig.Password)
		assert.Equal(t, "select * from cpu_load", node.(*QueryNode).Config.Command)
	})
	t.Run("OnMsg", func(t *testing.T) {
		server := "8.134.32.225:8086"
		node1, err := test.CreateAndInitNode(queryNodeType, types.Configuration{
			"server":   server,
			"database": "db0",
			"command":  "select * from cpu_load",
		}, Registry)
		assert.Nil(t, err)
		node2, _ := test.CreateAndInitNode(queryNodeType, types.Configuration{
			"server":   server,
			"database": "${metadata.database}",
			"command":  "select * from ${msg.table}",
		}, Registry)
		node3, _ := test.CreateAndInitNode(queryNodeType, types.Configuration{
			"server":   server,
			"database": "db0",
			"command":  "select * from xx",
		}, Registry)
		metaData := types.BuildMetadata(make(map[string]string))
		metaData.PutValue("database", "db0")
		msgList := []test.Msg{
			{
				MetaData: metaData,
				DataType: types.JSON,
				MsgType:  "cpu_load_err",
				Data:     "{\"table\":\"cpu_load\"}",
			},
		}
		var nodeList = []test.NodeAndCallback{
			{
				Node:    node1,
				MsgList: msgList,
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, types.Success, relationType)
				},
			}, {
				Node:    node2,
				MsgList: msgList,
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, types.Success, relationType)
				},
			}, {
				Node:    node3,
				MsgList: msgList,
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.True(t, strings.Contains(msg.GetData(), "measurement not found"))
					assert.Equal(t, types.Failure, relationType)
				},
			},
		}
		for _, item := range nodeList {
			test.NodeOnMsgWithChildren(t, item.Node, item.MsgList, item.ChildrenNodes, item.Callback)
		}
		time.Sleep(time.Second * 5)
	})
}
