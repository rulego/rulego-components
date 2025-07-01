# rulego-components

`rulego-components` 是 [RuleGo](https://github.com/rulego/rulego) 规则引擎扩展组件库。

## 特性

组件库分为以下子模块：

* **endpoint：** 接收端端点，负责监听并接收数据，然后交给`RuleGo`规则引擎处理。
  - [x/kafka](/endpoint/kafka/kafka.go) Kafka消息订阅端点
  - [x/redis](/endpoint/redis/redis.go) Redis发布/订阅端点
  - [x/redisStream](/endpoint/redis_stream/redis.go) Redis Stream消费端点
  - [x/nats](/endpoint/nats/nats.go) NATS消息订阅端点
  - [x/rabbitmq](/endpoint/rabbitmq/rabbitmq.go) RabbitMQ消息订阅端点
  - [x/fastHttp](/endpoint/fasthttp/fasthttp.go) FastHTTP服务器端点
  - [x/websocket](/endpoint/fasthttp/websocket.go) WebSocket服务器端点
  - [x/grpcStream](/endpoint/grpc_stream/grpc_stream.go) gRPC Stream服务器端点
  - [x/beanstalkd](/endpoint/beanstalkd/benstalkd.go) Beanstalkd任务队列端点
  - [x/wukongim](/endpoint/wukongim/wukongim.go) 悟空IM消息端点

* **filter：** 对消息进行过滤。
  - [x/luaFilter](/filter/lua_filter.go) 使用Lua脚本对消息进行过滤

* **transform：** 对消息进行转换。
  - [x/luaTransform](/transform/lua_transform.go) 使用Lua脚本对消息进行转换

* **external：** 外部集成，和第三方系统进行集成。
  - [x/kafkaProducer](/external/kafka/kafka_producer.go) Kafka生产者客户端
  - [x/redisClient](/external/redis/redis_client.go) Redis客户端
  - [x/redisPublisher](/external/redis/redis_publisher.go) Redis发布者
  - [x/rabbitmqClient](/external/rabbitmq/rabbitmq_client.go) RabbitMQ客户端
  - [x/natsClient](/external/nats/nats_client.go) NATS客户端
  - [x/grpcClient](/external/grpc/grpc_client.go) gRPC客户端
  - [x/restApiCall](/external/fasthttp/rest_api_call_node.go) HTTP/REST API调用客户端
  - [x/mongodbClient](/external/mongodb/mongodb_client.go) MongoDB客户端
  - [x/opengeminiQuery](/external/opengemini/query.go) OpenGemini查询客户端
  - [x/opengeminiWrite](/external/opengemini/write.go) OpenGemini写入客户端
  - [x/beanstalkdTube](/external/beanstalkd/tube_node.go) Beanstalkd管道操作
  - [x/beanstalkdWorker](/external/beanstalkd/worker_node.go) Beanstalkd工作者
  - [x/wukongimSender](/external/wukongim/wukongim_sender.go) 悟空IM消息发送器
  - [x/otelClient](/external/otel/otel_client.go) OpenTelemetry客户端

* **action：** 执行某些动作。
  - 暂无实现组件

* **stats：** 对数据进行统计、分析。
  - 暂无实现组件

## 安装

使用`go get`命令安装`rulego-components`：

```bash
go get github.com/rulego/rulego-components
```

## 使用

使用空白标识符导入扩展组件，扩展组件会自动注册到`RuleGo`：

```go
// 导入Endpoint组件
_ "github.com/rulego/rulego-components/endpoint/kafka"
_ "github.com/rulego/rulego-components/endpoint/redis"
_ "github.com/rulego/rulego-components/endpoint/fasthttp"

// 导入External组件
_ "github.com/rulego/rulego-components/external/redis"
_ "github.com/rulego/rulego-components/external/kafka"
_ "github.com/rulego/rulego-components/external/rabbitmq"

// 导入Filter和Transform组件
_ "github.com/rulego/rulego-components/filter"
_ "github.com/rulego/rulego-components/transform"
```

然后在规则链JSON文件使用组件指定的type调用扩展组件：

```json
{
  "ruleChain": {
    "id": "rule01",
    "name": "测试规则链"
  },
  "metadata": {
    "nodes": [
      {
        "id": "s1",
        "type": "x/redisClient",
        "name": "Redis客户端",
        "debugMode": true,
        "configuration": {
          "server": "127.0.0.1:6379",
          "password": "",
          "db": 0,
          "cmd": "SET",
          "key": "test-key",
          "value": "${msg}"
        }
      },
      {
        "id": "s2",
        "type": "x/luaFilter",
        "name": "Lua过滤器",
        "configuration": {
          "script": "return msg.temperature > 20"
        }
      }
    ],
    "connections": [
      {
        "fromId": "s1",
        "toId": "s2",
        "type": "Success"
      }
    ]
  }
}
```

## 组件类型说明

### Endpoint组件类型

| 组件 | 类型 | 描述 |
|------|------|------|
| Kafka | `x/kafka` | Kafka消息消费端点 |
| Redis | `x/redis` | Redis发布/订阅端点 |
| Redis Stream | `x/redisStream` | Redis Stream消费端点 |
| NATS | `x/nats` | NATS消息订阅端点 |
| RabbitMQ | `x/rabbitmq` | RabbitMQ消息消费端点 |
| FastHTTP | `x/fastHttp` | HTTP服务器端点 |
| WebSocket | `x/websocket` | WebSocket服务器端点 |
| gRPC Stream | `x/grpcStream` | gRPC流服务端点 |
| Beanstalkd | `x/beanstalkd` | Beanstalkd任务队列端点 |
| 悟空IM | `x/wukongim` | 悟空IM消息接收端点 |

### External组件类型

| 组件 | 类型 | 描述 |
|------|------|------|
| Kafka生产者 | `x/kafkaProducer` | 发送消息到Kafka |
| Redis客户端 | `x/redisClient` | 执行Redis命令 |
| Redis发布者 | `x/redisPublisher` | 发布消息到Redis |
| RabbitMQ客户端 | `x/rabbitmqClient` | 发送消息到RabbitMQ |
| NATS客户端 | `x/natsClient` | 发送消息到NATS |
| gRPC客户端 | `x/grpcClient` | 调用gRPC服务 |
| REST API调用 | `x/restApiCall` | 调用HTTP/REST API |
| MongoDB客户端 | `x/mongodbClient` | MongoDB数据库操作 |
| OpenGemini查询 | `x/opengeminiQuery` | 查询OpenGemini时序数据库 |
| OpenGemini写入 | `x/opengeminiWrite` | 写入OpenGemini时序数据库 |
| Beanstalkd管道 | `x/beanstalkdTube` | Beanstalkd管道操作 |
| Beanstalkd工作者 | `x/beanstalkdWorker` | Beanstalkd任务处理 |
| 悟空IM发送器 | `x/wukongimSender` | 发送消息到悟空IM |
| OpenTelemetry | `x/otelClient` | 发送遥测数据到OpenTelemetry |

### Filter和Transform组件类型

| 组件 | 类型 | 描述 |
|------|------|------|
| Lua过滤器 | `x/luaFilter` | 使用Lua脚本过滤消息 |
| Lua转换器 | `x/luaTransform` | 使用Lua脚本转换消息 |

## 贡献

`RuleGo` 的核心特性是组件化，所有业务逻辑都是组件，并能灵活配置和重用它们。目前，`RuleGo` 已经内置了一些常用的组件，如消息类型 Switch, JavaScript Switch, JavaScript 过滤器, JavaScript 转换器, HTTP 推送, MQTT 推送, 发送邮件, 日志记录 等组件。      

但是，我们知道这些组件还远远不能满足所有用户的需求，所以我们希望能有更多的开发者为 RuleGo 贡献扩展组件，让 RuleGo 的生态更加丰富和强大。     

如果你对 RuleGo 感兴趣，并且想要为它贡献扩展组件，你可以参考以下步骤：

- 阅读 RuleGo 的 [文档](https://rulego.cc) ，了解其架构、特性和使用方法。
- Fork RuleGo 的 [仓库](https://github.com/rulego/rulego) ，并 clone 到本地。
- 参考 RuleGo 的 [示例](https://github.com/rulego/rulego/tree/main/components) ，编写你自己的扩展组件，并实现相应的接口和方法。
- 在本地测试你的扩展组件，确保其功能正常且无误。
- 提交你的代码，并发起 pull request，我们会尽快审核并合并你的贡献。
- 在 GitHub/Gitee 上给 RuleGo 项目点个星星，让更多的人知道它。

我们非常欢迎和感谢任何形式的贡献，无论是代码、文档、建议还是反馈。我们相信，有了你们的支持，RuleGo 会成为一个更好的规则引擎和事件处理框架。谢谢！

如果您提交的组件代码没有第三方依赖或者是通用性组件请提交到：[github.com/rulego/rulego](https://github.com/rulego/rulego) 下的内置`components`,
否则提交到本仓库：[rulego-components](https://github.com/rulego/rulego-components) 。

## 交流群

QQ群号：**720103251**     
<img src="https://gitee.com/rulego/rulego/raw/main/doc/imgs/qq.png">

## 许可

`RuleGo`使用Apache 2.0许可证，详情请参见[LICENSE](LICENSE)文件。