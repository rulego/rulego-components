# rulego-components

`rulego-components` is a rule engine extension component library for [RuleGo](https://github.com/rulego/rulego).

## Features
The component library is divided into the following submodules:
* **endpoint:** Receiver endpoint, responsible for listening and receiving data, and then handing it over to the `RuleGo` rule engine for processing. For example: MQTT endpoint (subscribe to MQTT Broker data), HTTP endpoint (HTTP server)
* **filter:** Filter messages.
* **transform:** Transform messages.
* **action:** Perform some actions.
* **stats:** Perform statistics and analysis on data.
* **external:** External integration, integrate with third-party systems, such as: calling kafka, database, third-party api, etc.

## Installation

Use the `go get` command to install `RuleGo`:

```bash
go get github.com/rulego/rulego
```

Use the `go get` command to install `rulego-components`:

```bash
go get github.com/rulego/rulego-components/{submodel}
```


## Usage

Register the component to the `RuleGo` default registry
```go
rulego.Registry.Register(&MyNode{})
```

Then use your component in the rule chain DSL file
```json
{
  "ruleChain": {
    "name": "Test rule chain",
    "root": true,
    "debugMode": false
  },
  "metadata": {
    "nodes": [
      {
        "id": "s1",
        "type": "test/upper",
        "name": "Name",
        "debugMode": true,
        "configuration": {
          "field1": "Configuration parameters defined by the component",
          "....": "..."
        }
      }
    ],
    "connections": [
      {
        "fromId": "s1",
        "toId": "Connect to the next component ID",
        "type": "The connection relationship with the component"
      }
    ],
    "ruleChainConnections": null
  }
}
```

## Contributing

The core feature of `RuleGo` is componentization, where all business logic is composed of components, and they can be flexibly configured and reused. Currently, `RuleGo` has built-in some common components, such as message type Switch, JavaScript Switch, JavaScript Filter, JavaScript Transformer, HTTP Push, MQTT Push, Send Email, Log Record and so on.      
However, we know that these components are far from meeting the needs of all users, so we hope to have more developers contribute to RuleGo's extension components, making RuleGo's ecosystem more rich and powerful.     

If you are interested in RuleGo and want to contribute to its extension components, you can follow these steps:

- Read RuleGo's [documentation](https://github.com/rulego/rulego)  , and learn about its architecture, features and usage.
- Fork RuleGo's [repository](https://github.com/rulego/rulego)  , and clone it to your local machine.
- Refer to RuleGo's [examples](https://github.com/rulego/rulego/tree/main/components) , and write your own extension component, implementing the corresponding interfaces and methods.
- Test your extension component locally, and make sure it works properly and correctly.
- Submit your code and create a pull request, we will review and merge your contribution as soon as possible.
- Give a star to the RuleGo project on GitHub/Gitee, and let more people know about it.

We welcome and appreciate any form of contribution, whether it is code, documentation, suggestion or feedback. We believe that with your support, RuleGo will become a better rule engine and event processing framework. Thank you!

If the component code you submit has no third-party dependencies or is a general-purpose component, please submit it to the built-in `components` under [github.com/rulego/rulego](https://github.com/rulego/rulego) , otherwise submit it to this repository: [rulego-components](https://github.com/rulego/rulego-components) .

## License

`RuleGo` is licensed under the Apache 2.0 License - see the [LICENSE] file for details.
