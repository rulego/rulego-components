# rulego-components

`rulego-components` 是 [RuleGo](https://github.com/rulego/rulego) 规则引擎扩展组件库。

## 特性
组件库分为以下子模块：
* **endpoint：** 接收端端点，负责监听并接收数据，然后交给`RuleGo`规则引擎处理。例如：MQTT endpoint（订阅MQTT Broker 数据）、HTTP endpoint（HTTP server）
* **filter：** 对消息进行过滤。
* **transform：** 对消息进行转换。
* **action：** 执行某些动作。
* **stats：** 对数据进行统计、分析。
* **external：** 外部集成，和第三方系统进行集成，例如：调用kafka、数据库、第三方api等。

## 安装

使用`go get`命令安装`RuleGo`：

```bash
go get github.com/rulego/rulego
```

使用`go get`命令安装`rulego-components`：

```bash
go get github.com/rulego/rulego-components/{submodel}
```


## 使用

把组件注册到`RuleGo`默认注册器
```go
rulego.Registry.Register(&MyNode{})
```

然后在规则链DSL文件使用您的组件
```json
{
  "ruleChain": {
    "name": "测试规则链",
    "root": true,
    "debugMode": false
  },
  "metadata": {
    "nodes": [
      {
        "id": "s1",
        "type": "test/upper",
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

`RuleGo` 旨在创建一个开放的、丰富的生态系统，欢迎贡献扩展组件。如果您提交的组件代码没有第三方依赖或者是通用性组件请提交到[github.com/rulego/rulego/components](https://github.com/rulego/rulego) 下的内置`components`,
否则提交到本仓库。     

我们也欢迎任何形式的贡献，包括提交问题、建议、文档、测试或代码。请遵循以下步骤：

* 克隆项目仓库到本地
* 创建一个新的分支并进行修改
* 提交一个合并请求到主分支
* 等待审核和反馈

## 交流群

QQ群号：**720103251**     
<img src="https://gitee.com/rulego/rulego/raw/main/doc/imgs/qq.png">

## 许可

`RuleGo`使用Apache 2.0许可证，详情请参见[LICENSE](LICENSE)文件。