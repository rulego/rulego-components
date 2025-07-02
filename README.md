# rulego-components
[![Test](https://github.com/rulego/rulego-components/actions/workflows/test.yml/badge.svg)](https://github.com/rulego/rulego-components/actions/workflows/test.yml)

English| [简体中文](README_ZH.md)

`rulego-components` is a rule engine extension component library for [RuleGo](https://github.com/rulego/rulego).

## Features

The component library is divided into the following submodules:

* **endpoint:** Receiver endpoint, responsible for listening and receiving data, and then handing it over to the `RuleGo` rule engine for processing.
  - [x/kafka](/endpoint/kafka/kafka.go) Kafka message subscription endpoint
  - [x/redis](/endpoint/redis/redis.go) Redis publish/subscribe endpoint
  - [x/redisStream](/endpoint/redis_stream/redis.go) Redis Stream consumption endpoint
  - [x/nats](/endpoint/nats/nats.go) NATS message subscription endpoint
  - [x/rabbitmq](/endpoint/rabbitmq/rabbitmq.go) RabbitMQ message subscription endpoint
  - [x/fastHttp](/endpoint/fasthttp/fasthttp.go) FastHTTP server endpoint
  - [x/websocket](/endpoint/fasthttp/websocket.go) WebSocket server endpoint
  - [x/grpcStream](/endpoint/grpc_stream/grpc_stream.go) gRPC Stream server endpoint
  - [x/beanstalkd](/endpoint/beanstalkd/benstalkd.go) Beanstalkd task queue endpoint
  - [x/wukongim](/endpoint/wukongim/wukongim.go) WuKongIM message endpoint

* **filter:** Filter the messages.
  - [x/luaFilter](/filter/lua_filter.go) Use Lua script to filter messages

* **transform:** Transform the messages.
  - [x/luaTransform](/transform/lua_transform.go) Use Lua script to transform messages

* **external:** External integration, integrate with third-party systems.
  - [x/kafkaProducer](/external/kafka/kafka_producer.go) Kafka producer client
  - [x/redisClient](/external/redis/redis_client.go) Redis client
  - [x/redisPublisher](/external/redis/redis_publisher.go) Redis publisher
  - [x/rabbitmqClient](/external/rabbitmq/rabbitmq_client.go) RabbitMQ client
  - [x/natsClient](/external/nats/nats_client.go) NATS client
  - [x/grpcClient](/external/grpc/grpc_client.go) gRPC client
  - [x/restApiCall](/external/fasthttp/rest_api_call_node.go) HTTP/REST API call client
  - [x/mongodbClient](/external/mongodb/mongodb_client.go) MongoDB client
  - [x/opengeminiQuery](/external/opengemini/query.go) OpenGemini query client
  - [x/opengeminiWrite](/external/opengemini/write.go) OpenGemini write client
  - [x/beanstalkdTube](/external/beanstalkd/tube_node.go) Beanstalkd tube operations
  - [x/beanstalkdWorker](/external/beanstalkd/worker_node.go) Beanstalkd worker
  - [x/wukongimSender](/external/wukongim/wukongim_sender.go) WuKongIM message sender
  - [x/otelClient](/external/otel/otel_client.go) OpenTelemetry client

* **action:** Perform some actions.
  - No components implemented yet

* **stats:** Perform statistics and analysis on the data.
  - No components implemented yet

## Installation

Use the `go get` command to install `rulego-components`:

```bash
go get github.com/rulego/rulego-components
```

## Usage

Use the blank identifier to import the extension component, and the extension component will automatically register to `RuleGo`:

```go
// Import Endpoint components
_ "github.com/rulego/rulego-components/endpoint/kafka"
_ "github.com/rulego/rulego-components/endpoint/redis"
_ "github.com/rulego/rulego-components/endpoint/fasthttp"

// Import External components
_ "github.com/rulego/rulego-components/external/redis"
_ "github.com/rulego/rulego-components/external/kafka"
_ "github.com/rulego/rulego-components/external/rabbitmq"

// Import Filter and Transform components
_ "github.com/rulego/rulego-components/filter"
_ "github.com/rulego/rulego-components/transform"
```

Then use the type specified by the component in the rule chain JSON file to call the extension component:

```json
{
  "ruleChain": {
    "id": "rule01",
    "name": "Test rule chain"
  },
  "metadata": {
    "nodes": [
      {
        "id": "s1",
        "type": "x/redisClient",
        "name": "Redis Client",
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
        "name": "Lua Filter",
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

## Component Type Reference

### Endpoint Component Types

| Component | Type | Description |
|-----------|------|-------------|
| Kafka | `x/kafka` | Kafka message consumption endpoint |
| Redis | `x/redis` | Redis publish/subscribe endpoint |
| Redis Stream | `x/redisStream` | Redis Stream consumption endpoint |
| NATS | `x/nats` | NATS message subscription endpoint |
| RabbitMQ | `x/rabbitmq` | RabbitMQ message consumption endpoint |
| FastHTTP | `x/fastHttp` | HTTP server endpoint |
| WebSocket | `x/websocket` | WebSocket server endpoint |
| gRPC Stream | `x/grpcStream` | gRPC stream server endpoint |
| Beanstalkd | `x/beanstalkd` | Beanstalkd task queue endpoint |
| WuKongIM | `x/wukongim` | WuKongIM message receiving endpoint |

### External Component Types

| Component | Type | Description |
|-----------|------|-------------|
| Kafka Producer | `x/kafkaProducer` | Send messages to Kafka |
| Redis Client | `x/redisClient` | Execute Redis commands |
| Redis Publisher | `x/redisPublisher` | Publish messages to Redis |
| RabbitMQ Client | `x/rabbitmqClient` | Send messages to RabbitMQ |
| NATS Client | `x/natsClient` | Send messages to NATS |
| gRPC Client | `x/grpcClient` | Call gRPC services |
| REST API Call | `x/restApiCall` | Call HTTP/REST APIs |
| MongoDB Client | `x/mongodbClient` | MongoDB database operations |
| OpenGemini Query | `x/opengeminiQuery` | Query OpenGemini time-series database |
| OpenGemini Write | `x/opengeminiWrite` | Write to OpenGemini time-series database |
| Beanstalkd Tube | `x/beanstalkdTube` | Beanstalkd tube operations |
| Beanstalkd Worker | `x/beanstalkdWorker` | Beanstalkd task processing |
| WuKongIM Sender | `x/wukongimSender` | Send messages to WuKongIM |
| OpenTelemetry | `x/otelClient` | Send telemetry data to OpenTelemetry |

### Filter and Transform Component Types

| Component | Type | Description |
|-----------|------|-------------|
| Lua Filter | `x/luaFilter` | Filter messages using Lua script |
| Lua Transform | `x/luaTransform` | Transform messages using Lua script |

## Contributing

The core feature of `RuleGo` is componentization, where all business logic is composed of components, and they can be flexibly configured and reused. Currently, `RuleGo` has built-in some common components, such as message type Switch, JavaScript Switch, JavaScript Filter, JavaScript Transformer, HTTP Push, MQTT Push, Send Email, Log Record and so on.      

However, we know that these components are far from meeting the needs of all users, so we hope to have more developers contribute to RuleGo's extension components, making RuleGo's ecosystem more rich and powerful.     

If you are interested in RuleGo and want to contribute to its extension components, you can follow these steps:

- Read RuleGo's [documentation](https://rulego.cc), and learn about its architecture, features and usage.
- Fork RuleGo's [repository](https://github.com/rulego/rulego), and clone it to your local machine.
- Refer to RuleGo's [examples](https://github.com/rulego/rulego/tree/main/components), and write your own extension component, implementing the corresponding interfaces and methods.
- Test your extension component locally, and make sure it works properly and correctly.
- Submit your code and create a pull request, we will review and merge your contribution as soon as possible.
- Give a star to the RuleGo project on GitHub/Gitee, and let more people know about it.

We welcome and appreciate any form of contribution, whether it is code, documentation, suggestion or feedback. We believe that with your support, RuleGo will become a better rule engine and event processing framework. Thank you!

If the component code you submit has no third-party dependencies or is a general-purpose component, please submit it to the built-in `components` under [github.com/rulego/rulego](https://github.com/rulego/rulego), otherwise submit it to this repository: [rulego-components](https://github.com/rulego/rulego-components).

## License

`RuleGo` is licensed under the Apache 2.0 License - see the [LICENSE] file for details.
