# rulego-components

`rulego-components` 是 [RuleGo](https://github.com/rulego/rulego) 规则引擎扩展组件库。

## 特性
组件库分为以下子模块：
* **endpoint：** 接收端端点，负责监听并接收数据，然后交给`RuleGo`规则引擎处理。例如：MQTT Endpoint（订阅MQTT Broker 数据）、REST Endpoint（HTTP Server）、Websocket Endpoint、Kafka Endpoint
* **filter：** 对消息进行过滤。
* **transform：** 对消息进行转换。
* **action：** 执行某些动作。
* **stats：** 对数据进行统计、分析。
* **external：** 外部集成，和第三方系统进行集成，例如：调用kafka、数据库、第三方api等。

## 安装

使用`go get`命令安装`rulego-components`：

```bash
go get github.com/rulego/rulego-components
```

## 使用

使用空白标识符导入扩展组件，扩展组件会自动注册到`RuleGo`
```go
_ "github.com/rulego/rulego-components/external/redis"
```

然后在规则链JSON文件使用组件指定的type调用扩展组件
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
        "name": "名称",
        "debugMode": true,
        "configuration": {
          "field1": "组件定义的配置参数",
          "....": "..."
        }
      }
    ],
    "connections": [
      {
        "fromId": "s1",
        "toId": "连接下一个组件ID",
        "type": "与组件的连接关系"
      }
    ],
    "ruleChainConnections": null
  }
}
```

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