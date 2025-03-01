/*
 * Copyright 2024 The RuleGo Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mongodb

import (
	"context"
	"errors"
	"fmt"
	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
	"strconv"
	"strings"

	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/str"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	SELECT = "SELECT"
	QUERY  = "QUERY"
	INSERT = "INSERT"
	DELETE = "DELETE"
	UPDATE = "UPDATE"
)
const (
	MatchedCount  = "matchedCount"
	ModifiedCount = "modifiedCount"
	DeletedCount  = "deletedCount"
)

// 注册节点
func init() {
	_ = rulego.Registry.Register(&ClientNode{})
}

// ClientNodeConfiguration 节点配置
type ClientNodeConfiguration struct {
	// 服务器地址， 示例：mongodb://username:password@localhost:27017/?authSource=admin
	Server string
	//数据库名称，支持${}占位符
	Database string
	//集合名称，支持${}占位符
	Collection string
	//操作类型 INSERT,UPDATE,DELETE,QUERY
	OpType string
	// 过滤条件，可以使用expr表达式。示例：{"age": {"$gte": 18}}
	Filter string
	//更新/插入文档。可以使用expr表达式。示例：{"name":"test","age":18}
	Doc string
	// 是否只操作一条数据
	One bool
}

// ClientNode mongodb客户端组件，可以对mongodb进行增删改查操作
type ClientNode struct {
	base.SharedNode[*mongo.Client]
	// 节点配置
	Config ClientNodeConfiguration
	client *mongo.Client
	// 数据库名称
	DatabaseNameTemplate str.Template
	// 集合名称
	CollectionNameTemplate str.Template
	// 过滤
	FilterTemplate *vm.Program
	// 文档
	DocTemplate *vm.Program
}

// Type 返回组件类型
func (x *ClientNode) Type() string {
	return "x/mongodbClient"
}

func (x *ClientNode) New() types.Node {
	return &ClientNode{Config: ClientNodeConfiguration{
		Server:     "mongodb://localhost:27017",
		Database:   "test",
		Collection: "user",
		OpType:     QUERY,
		Filter:     `{"age": {"$gte": 18}}`,
	}}
}

// Init 初始化组件
func (x *ClientNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	if err != nil {
		return err
	}
	if x.Config.Server == "" {
		return errors.New("server can not be empty")
	}
	if x.Config.Database == "" {
		return errors.New("databaseName can not be empty")
	} else {
		x.DatabaseNameTemplate = str.NewTemplate(strings.TrimSpace(x.Config.Database))
	}
	if x.Config.Collection == "" {
		return errors.New("collectionName can not be empty")
	} else {
		x.CollectionNameTemplate = str.NewTemplate(strings.TrimSpace(x.Config.Collection))
	}
	if x.Config.OpType == "" {
		return errors.New("opType can not be empty")
	}
	if x.Config.Filter != "" {
		if program, err := expr.Compile(strings.TrimSpace(x.Config.Filter), expr.AllowUndefinedVariables()); err != nil {
			return err
		} else {
			x.FilterTemplate = program
		}
	}
	if x.Config.Doc != "" {
		if program, err := expr.Compile(strings.TrimSpace(x.Config.Doc), expr.AllowUndefinedVariables()); err != nil {
			return err
		} else {
			x.DocTemplate = program
		}
	}
	// 初始化客户端
	_ = x.SharedNode.Init(ruleConfig, x.Type(), x.Config.Server, ruleConfig.NodeClientInitNow, func() (*mongo.Client, error) {
		return x.initClient()
	})
	return nil
}

// OnMsg 处理消息
func (x *ClientNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	if client, err := x.SharedNode.Get(); err != nil {
		ctx.TellFailure(msg, err)
	} else {
		evn := base.NodeUtils.GetEvnAndMetadata(ctx, msg)

		databaseName := x.DatabaseNameTemplate.Execute(evn)
		collectionName := x.CollectionNameTemplate.Execute(evn)
		collection := client.Database(databaseName).Collection(collectionName)
		x.processMessage(ctx, evn, collection, msg, x.Config.OpType)
	}
}

// ProcessMessage 处理消息，执行查询、更新或删除操作
func (x *ClientNode) processMessage(ctx types.RuleContext, evn map[string]interface{}, collection *mongo.Collection, msg types.RuleMsg, opType string) {
	// 根据操作类型执行不同的操作
	switch opType {
	case INSERT:
		x.insert(ctx, evn, collection, msg)
	case QUERY, SELECT:
		x.query(ctx, evn, collection, msg)
	case UPDATE:
		x.update(ctx, evn, collection, msg)
	case DELETE:
		x.delete(ctx, evn, collection, msg)
	default:
		ctx.TellFailure(msg, fmt.Errorf("unsupported operation type: %s", opType))
	}
}

// Destroy 销毁组件
func (x *ClientNode) Destroy() {
	if x.client != nil {
		_ = x.client.Disconnect(context.TODO())
	}
}

func (x *ClientNode) toBsonM(evn map[string]interface{}, template *vm.Program) (interface{}, error) {
	if out, err := vm.Run(template, evn); err != nil {
		return nil, err
	} else {
		if r, ok := out.(map[string]interface{}); ok {
			return r, nil
		} else {
			return nil, errors.New("expr result is not map[string]interface{}")
		}
	}
}

func (x *ClientNode) toBsonMList(evn map[string]interface{}, template *vm.Program) ([]interface{}, error) {
	if out, err := vm.Run(template, evn); err != nil {
		return nil, err
	} else {
		if r, ok := out.([]interface{}); ok {
			return r, nil
		} else if r, ok := out.(map[string]interface{}); ok {
			var interfaceList []interface{}
			interfaceList = append(interfaceList, r)
			return interfaceList, nil
		} else if r, ok := out.([]map[string]interface{}); ok {
			var interfaceList []interface{}
			for _, item := range r {
				interfaceList = append(interfaceList, item)
			}
			return interfaceList, nil
		} else {
			return nil, errors.New("expr result is not []map[string]interface{} or []interface{}")
		}
	}
}

func (x *ClientNode) insert(ctx types.RuleContext, evn map[string]interface{}, collection *mongo.Collection, msg types.RuleMsg) {
	if x.Config.One {
		if doc, err := x.toBsonM(evn, x.DocTemplate); err != nil {
			ctx.TellFailure(msg, err)
		} else {
			// 插入文档
			_, err = collection.InsertOne(ctx.GetContext(), doc)
			ctx.TellSuccess(msg)
		}
	} else {
		if docs, err := x.toBsonMList(evn, x.DocTemplate); err != nil {
			ctx.TellFailure(msg, err)
		} else {
			// 插入多个文档
			_, err = collection.InsertMany(ctx.GetContext(), docs)
			if err != nil {
				ctx.TellFailure(msg, err)
			} else {
				ctx.TellSuccess(msg)
			}
		}
	}
}

func (x *ClientNode) query(ctx types.RuleContext, evn map[string]interface{}, collection *mongo.Collection, msg types.RuleMsg) {
	if filter, err := x.toBsonM(evn, x.FilterTemplate); err != nil {
		ctx.TellFailure(msg, err)
	} else {
		if x.Config.One {
			// 查询单个文档
			var result bson.M
			if err := collection.FindOne(ctx.GetContext(), filter).Decode(&result); err != nil {
				ctx.TellFailure(msg, err)
			} else {
				msg.Data = str.ToString(result)
				ctx.TellSuccess(msg)
			}
		} else {
			// 查询文档列表
			cursor, err := collection.Find(ctx.GetContext(), filter)
			if err != nil {
				ctx.TellFailure(msg, err)
				return
			}
			defer cursor.Close(ctx.GetContext())
			var results []bson.M
			if err = cursor.All(ctx.GetContext(), &results); err != nil {
				ctx.TellFailure(msg, err)
			} else {
				msg.Data = str.ToString(results)
				ctx.TellSuccess(msg)
			}
		}
	}
}
func (x *ClientNode) update(ctx types.RuleContext, evn map[string]interface{}, collection *mongo.Collection, msg types.RuleMsg) {
	var err error
	var doc interface{}
	var filter interface{}

	if doc, err = x.toBsonM(evn, x.DocTemplate); err != nil {
		ctx.TellFailure(msg, err)
		return
	}
	if filter, err = x.toBsonM(evn, x.FilterTemplate); err != nil {
		ctx.TellFailure(msg, err)
		return
	}
	if x.Config.One {
		// 更新单个文档
		if updateResult, err := collection.UpdateOne(ctx.GetContext(), filter, bson.M{"$set": doc}); err != nil {
			ctx.TellFailure(msg, err)
		} else {
			msg.Metadata.PutValue(MatchedCount, strconv.FormatInt(updateResult.MatchedCount, 10))
			msg.Metadata.PutValue(ModifiedCount, strconv.FormatInt(updateResult.ModifiedCount, 10))
			ctx.TellSuccess(msg)
		}
	} else {
		if updateResult, err := collection.UpdateMany(ctx.GetContext(), filter, bson.M{"$set": doc}); err != nil {
			ctx.TellFailure(msg, err)
		} else {
			msg.Metadata.PutValue(MatchedCount, strconv.FormatInt(updateResult.MatchedCount, 10))
			msg.Metadata.PutValue(ModifiedCount, strconv.FormatInt(updateResult.ModifiedCount, 10))
			ctx.TellSuccess(msg)
		}
	}

}
func (x *ClientNode) delete(ctx types.RuleContext, evn map[string]interface{}, collection *mongo.Collection, msg types.RuleMsg) {
	if filter, err := x.toBsonM(evn, x.FilterTemplate); err != nil {
		ctx.TellFailure(msg, err)
	} else {
		if x.Config.One {
			// 删除文档
			if deleteResult, err := collection.DeleteOne(ctx.GetContext(), filter); err != nil {
				ctx.TellFailure(msg, err)
				return
			} else {
				msg.Metadata.PutValue(DeletedCount, strconv.FormatInt(deleteResult.DeletedCount, 10))
				ctx.TellSuccess(msg)
			}
		} else {
			if deleteResult, err := collection.DeleteMany(ctx.GetContext(), filter); err != nil {
				ctx.TellFailure(msg, err)
				return
			} else {
				msg.Metadata.PutValue(DeletedCount, strconv.FormatInt(deleteResult.DeletedCount, 10))
				ctx.TellSuccess(msg)
			}
		}
	}
}

// initClient 初始化客户端
func (x *ClientNode) initClient() (*mongo.Client, error) {
	if x.client != nil {
		return x.client, nil
	} else {
		x.Locker.Lock()
		defer x.Locker.Unlock()
		if x.client != nil {
			return x.client, nil
		}
		var err error
		client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(x.Config.Server))
		if err == nil {
			x.client = client
			// 测试连接
			err = x.client.Ping(context.TODO(), nil)
		}
		return x.client, err
	}
}
