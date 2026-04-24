# NSQ Endpoint（RuleGo 端点）

基于 [go-nsq](https://github.com/nsqio/go-nsq) 的 NSQ 端点：从指定 **topic** 消费消息并驱动规则链；在需要时通过 **元数据或响应头** 将处理结果**发布**到另一 topic。支持直连 **nsqd**、通过 **lookupd** 发现地址，以及多 nsqd 上的**轮询发布**与失败重试。

在代码中请使用本包导出的 **`Type`** 常量（`types.EndpointTypePrefix + "nsq"`）作为 `endpoint.Registry` 的组件类型，勿手写字符串，以免与 RuleGo 版本中的前缀不一致。

## 配置项

| 字段 | 说明 |
|------|------|
| `server` | **必填**。NSQ 地址，见下文「`server` 写法」 |
| `channel` | 默认 channel；`AddRouter` 未传参时使用 |
| `authToken` | 与 nsqd 一致的鉴权 Secret（若集群启用鉴权） |
| `certFile` / `certKeyFile` | 预留的 TLS 证书配置（与当前 go-nsq 消费者/生产者用法一致时再接） |

## `server` 写法

- **单 nsqd**：`127.0.0.1:4150`（进程内对该地址建连；发布时仅此一路）
- **多 nsqd**（英文逗号分隔）：`10.0.0.1:4150,10.0.0.2:4150`  
  会对**每个**可达地址建 `Producer`；**消费**走 `ConnectToNSQDs`；**发布**在进程内 **round-robin**，单条 `Publish` 失败时在同一次调用中轮询重试其他节点
- **lookupd**（需 `http://` 或 `https://`）：`http://127.0.0.1:4161` 或 `http://a:4161,http://b:4161`  
  按顺序请求各 lookupd 的 `/nodes`，在**第一次**成功返回非空 nsqd 列表后，对该列表中可达的 nsqd 建连，发布行为同上

## 使用步骤（概念）

1. 在 RuleGo 中注册并 **Init** 本端点，传入 `Config`（至少 `Server`）
2. **AddRouter**：`router.From` 的字符串为要订阅的 **NSQ topic**；第二个参数可覆盖 **channel**
3. 规则链处理入站消息；若要把结果写回 NSQ，在出站侧设置 **响应 topic** 与 body（见下）

## 参考示例 1：Go 中注册与路由

与仓库内 `endpoint/nsq/nsq_test.go` 一致：先 `rulego.New` 注册规则链，再 `endpoint.Registry.New` 创建端点（第三个参数为 `nsq.Config`，内部会完成 `Init`）。

```go
import (
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	endpointapi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/endpoint"
	nsqend "github.com/rulego/rulego-components/endpoint/nsq"
)

func example(cfg types.Config) error {
	// 已加载好规则链，例如: _, _ = rulego.New("default", chainBytes, rulego.WithConfig(cfg))

	ep, err := endpoint.Registry.New(nsqend.Type, cfg, nsqend.Config{
		Server:  "127.0.0.1:4150", // 或多 nsqd / lookupd，见上文
		Channel: "default",
		// AuthToken: "your-secret",
	})
	if err != nil {
		return err
	}

	router := endpoint.NewRouter().From("device_msg_request"). // 订阅的 NSQ topic
		Process(func(r endpointapi.Router, ex *endpointapi.Exchange) bool {
			// 处理入站：exchange.In.GetMsg() …
			return true
		}).
		To("chain:default"). // 进入规则链
		Process(func(r endpointapi.Router, ex *endpointapi.Exchange) bool {
			// 将结果发布到另一 topic（与 KeyResponseTopic 一致）
			ex.Out.Headers().Add(nsqend.KeyResponseTopic, "device_msg_response")
			ex.Out.SetBody([]byte(`this is response`))
			return true
		}).End()

	// 第二个参数为 channel，覆盖 Config.Channel；可省略以使用配置中的 channel
	_, err = ep.AddRouter(router, "channel1")
	if err != nil {
		return err
	}
	return ep.Start()
}
```

> 若从 map / JSON 注入配置，通常先构造 `types.Configuration` 再 `maps.Map2Struct` 到 `nsq.Config` 后注册；具体以你项目里其它 endpoint 的用法为准。

## 参考示例 2：从规则消息元数据指定响应 topic

`SetBody` 时会从 **metadata 的 `responseTopic`** 或 **Header 的 `responseTopic`** 取目标 topic，常量名为 `KeyResponseTopic`（`"responseTopic"`）：

```go
.Process(func(r endpointapi.Router, ex *endpointapi.Exchange) bool {
	if ex.In.GetMsg() != nil {
		// 也可在规则链前序节点里写入 Metadata
		ex.In.GetMsg().Metadata.PutValue(nsqend.KeyResponseTopic, "replies")
	}
	ex.Out.SetMsg(ex.In.GetMsg()) // 若需沿用同一 msg 的 metadata
	ex.Out.SetBody([]byte("pong"))
	return true
})
```

## 与 RuleGo JSON / 低代码

在可视化或 JSON 中配置端点时，将 `server`（及可选的 `channel`、`authToken`）与节点类型 `rulego/endpoint/nsq`（以你实际 RuleGo 的 endpoint 类型名为准）绑定；路由的 **From** 对应 NSQ **topic**。

示例（**结构示意**，字段名以你项目里 endpoint 的 schema 为准）：

```json
{
  "id": "nsq_in",
  "type": "nsq",
  "config": {
    "server": "http://lookup-1:4161,http://lookup-2:4161",
    "channel": "default"
  }
}
```

## 行为与限制（简）

- 多 **lookupd** 不合并多路返回的并集，只取**第一次**非空发现结果；多 **nsqd** 会在 Init 时尽量连上全部可达实例再轮询发布。
- **新 nsqd 进程**在 Init 之后才加入集群时，不会自动进池，一般需要**重启端点**或做运维侧摘挂。
- 更细的语义（与 NSQ、跨进程均衡等）见下节，或阅读源码中 `roundRobinProducers` / `buildReachableProducers` / `discoverNsqdProducersFromLookupds`。

### 历史问题与现实现要点（运维向）

- 多 lookupd、多 nsqd 的发现与**仅连第一个**的问题已用顺序尝试、全量 Ping 建池解决。
- 运行期在**本进程**内用轮询 + 同次重试降低单点发布压力；**跨**进程/多实例的全局是否均匀仍与各自发现结果与实例数有关。

## 本地跑单元测试

本仓库中 NSQ 相关用例在**未启动 nsqd** 时部分会 `Skip`。需要时：

```bash
export NSQD_ADDRESS=127.0.0.1:4150
export LOOKUPD_ADDRESS=http://127.0.0.1:4161
go test -count=1 ./endpoint/nsq/...
# 不跑需真实 NSQ 的用例：export SKIP_NSQ_TESTS=true
```

---

## 与 RuleGo 模块的 import 路径

在业务仓库中应使用本仓库 module 路径，例如：

```text
github.com/rulego/rulego-components/endpoint/nsq
```

若你 fork 或内网坐标不同，请替换为实际 `go.mod` 中的 module 路径。
