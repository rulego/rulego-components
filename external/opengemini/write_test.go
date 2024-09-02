package opengemini

import (
	"github.com/openGemini/opengemini-client-go/opengemini"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/test"
	"github.com/rulego/rulego/test/assert"
	"github.com/rulego/rulego/utils/json"
	"testing"
	"time"
)

func TestWriteNode(t *testing.T) {
	Registry := &types.SafeComponentSlice{}
	Registry.Add(&WriteNode{})
	Registry.Add(&QueryNode{})
	var writeNodeType = "x/opengeminiWrite"

	t.Run("NewNode", func(t *testing.T) {
		test.NodeNew(t, writeNodeType, &WriteNode{}, types.Configuration{
			"server":   "127.0.0.1:8086",
			"database": "db0",
		}, Registry)
	})

	t.Run("InitNode", func(t *testing.T) {
		node, _ := test.CreateAndInitNode(writeNodeType, types.Configuration{
			"server":   "127.0.0.1:8086,127.0.0.1:8087",
			"database": "db0",
			"token":    "aaa",
		}, Registry)
		assert.Equal(t, "aaa", node.(*WriteNode).opengeminiConfig.AuthConfig.Token)
		assert.Equal(t, "127.0.0.1", node.(*WriteNode).opengeminiConfig.Addresses[0].Host)
		assert.Equal(t, 8086, node.(*WriteNode).opengeminiConfig.Addresses[0].Port)
		assert.Equal(t, "127.0.0.1", node.(*WriteNode).opengeminiConfig.Addresses[1].Host)
		assert.Equal(t, 8087, node.(*WriteNode).opengeminiConfig.Addresses[1].Port)
		assert.Equal(t, opengemini.AuthTypeToken, node.(*WriteNode).opengeminiConfig.AuthConfig.AuthType)

		node, _ = test.CreateAndInitNode(writeNodeType, types.Configuration{
			"server":   "127.0.0.1:8086,192.168.0.1:8087",
			"database": "db0",
			"username": "aaa",
			"password": "bbb",
		}, Registry)
		assert.Equal(t, "", node.(*WriteNode).opengeminiConfig.AuthConfig.Token)
		assert.Equal(t, "127.0.0.1", node.(*WriteNode).opengeminiConfig.Addresses[0].Host)
		assert.Equal(t, 8086, node.(*WriteNode).opengeminiConfig.Addresses[0].Port)
		assert.Equal(t, "192.168.0.1", node.(*WriteNode).opengeminiConfig.Addresses[1].Host)
		assert.Equal(t, 8087, node.(*WriteNode).opengeminiConfig.Addresses[1].Port)
		assert.Equal(t, opengemini.AuthTypePassword, node.(*WriteNode).opengeminiConfig.AuthConfig.AuthType)
		assert.Equal(t, "aaa", node.(*WriteNode).opengeminiConfig.AuthConfig.Username)
		assert.Equal(t, "bbb", node.(*WriteNode).opengeminiConfig.AuthConfig.Password)
	})
	t.Run("OnMsg", func(t *testing.T) {
		server := "8.134.32.225:8086"
		node, err := test.CreateAndInitNode(writeNodeType, types.Configuration{
			"server":   server,
			"database": "db0",
		}, Registry)
		assert.Nil(t, err)
		node2, err := test.CreateAndInitNode(writeNodeType, types.Configuration{
			"server":   server,
			"database": "${database}",
		}, Registry)

		node3, err := test.CreateAndInitNode(writeNodeType, types.Configuration{
			"server":   server,
			"database": "aa",
		}, Registry)

		insertPoint1 := opengemini.Point{
			Measurement: "cpu_load",
			Tags: map[string]string{
				"host": "server01",
			},
			Fields: map[string]interface{}{
				"value": 98.6,
			},
		}
		insertPoint2 := opengemini.Point{
			Measurement: "cpu_load",
			Tags: map[string]string{
				"host": "server01",
			},
			Fields: map[string]interface{}{
				"value": 98.6,
			},
			Time: time.Now(),
		}
		insertPoints := []opengemini.Point{insertPoint1, insertPoint2}
		insertData1, _ := json.Marshal(insertPoint1)
		insertData2, _ := json.Marshal(insertPoints)
		insertData3 := "[{\"measurement\":\"cpu_load\",\"Precision\":0,\"Time\":\"2024-09-03T13:41:27.3142051+08:00\",\"Tags\":{\"host\":\"server01\"},\"Fields\":{\"value\":98.6}}]"
		lineProtocol := "cpu_load,host=server01,region=us-west value=23.5 1434055562000000000"
		metaData := types.BuildMetadata(make(map[string]string))
		metaData.PutValue("database", "db0")
		msgList := []test.Msg{
			{
				MetaData: metaData,
				DataType: types.TEXT,
				MsgType:  "cpu_load_err",
				Data:     "AA",
			},
			{
				MetaData: metaData,
				DataType: types.TEXT,
				MsgType:  "cpu_load_line_protocol",
				Data:     lineProtocol,
			},
			{
				MetaData: metaData,
				DataType: types.JSON,
				MsgType:  "cpu_load_json",
				Data:     string(insertData1),
			},
			{
				MetaData: metaData,
				DataType: types.JSON,
				MsgType:  "cpu_load_json",
				Data:     string(insertData2),
			},
			{
				MetaData: metaData,
				DataType: types.JSON,
				MsgType:  "cpu_load_json",
				Data:     insertData3,
			},
		}

		var nodeList = []test.NodeAndCallback{
			{
				Node:    node,
				MsgList: msgList,
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					if msg.Type == "cpu_load_err" {
						assert.Equal(t, types.Failure, relationType)
					} else {
						assert.Equal(t, types.Success, relationType)
					}
				},
			},
			{
				Node:    node2,
				MsgList: msgList,
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					if msg.Type == "cpu_load_err" {
						assert.Equal(t, types.Failure, relationType)
					} else {
						assert.Equal(t, types.Success, relationType)
					}

				},
			},
			{
				Node:    node3,
				MsgList: msgList,
				Callback: func(msg types.RuleMsg, relationType string, err error) {
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
