# FastHTTP Endpoint for RuleGo

基于 [fasthttp](https://github.com/valyala/fasthttp) 库实现的高性能 `HTTP endpoint` 组件，为 RuleGo 框架提供更快的 HTTP 请求处理能力。
可以无感代替标准 Http endpoint 组件，提供更快的 HTTP 请求处理能力。

## 特性

- **高性能**: 基于 fasthttp 库，比标准 net/http 库性能更高
- **低内存占用**: 减少内存分配，提高 GC 效率
- **高并发**: 支持更高的并发连接数
- **兼容性**: 与 RuleGo 原有的 HTTP endpoint 接口完全兼容
- **配置灵活**: 支持丰富的服务器配置选项
- **CORS 支持**: 内置跨域资源共享支持

## 安装

```bash
go get github.com/valyala/fasthttp
go get github.com/fasthttp/router
```

## 基本使用

```go
package main

import (
    "github.com/rulego/rulego/api/types"
    "github.com/rulego/rulego/endpoint/impl"
    "github.com/rulego/rulego/engine"
    "github.com/rulego/rulego/utils/maps"
    fasthttp "github.com/rulego/rulego-components/endpoint/fasthttp"
)

func main() {
    config := engine.NewConfig(types.WithDefaultPool())
    
    // 创建 FastHTTP endpoint
    var nodeConfig = make(types.Configuration)
    _ = maps.Map2Struct(&fasthttp.Config{
        Server: ":8080",
        AllowCors: true,
        Concurrency: 1000,
    }, &nodeConfig)
    
    endpoint := &fasthttp.Endpoint{}
    err := endpoint.Init(config, nodeConfig)
    if err != nil {
        panic(err)
    }
    
    // 添加路由
    router := impl.NewRouter().From("/api/test").Transform(func(ctx types.RuleContext, msg types.RuleMsg) types.RuleMsg {
        msg.Data = `{"status":"success","data":` + msg.Data + `}`
        return msg
    }).End()
    
    _, err = endpoint.AddRouter(router, "POST")
    if err != nil {
        panic(err)
    }
    
    // 启动服务
    err = endpoint.Start()
    if err != nil {
        panic(err)
    }
    
    // 保持服务运行
    select {}
}
```

## 配置选项

```go
type Config struct {
    Server           string        // 服务器地址，如 ":8080"
    CertFile         string        // TLS 证书文件路径
    CertKeyFile      string        // TLS 私钥文件路径
    AllowCors        bool          // 是否允许跨域
    ReadTimeout      time.Duration // 读取超时时间
    WriteTimeout     time.Duration // 写入超时时间
    IdleTimeout      time.Duration // 空闲超时时间
    MaxRequestSize   int           // 最大请求体大小
    Concurrency      int           // 最大并发连接数
    DisableKeepalive bool          // 是否禁用 Keep-Alive
}
```

### 默认配置

```go
Config{
    Server:           ":6334",
    ReadTimeout:      10 * time.Second,
    WriteTimeout:     10 * time.Second,
    IdleTimeout:      60 * time.Second,
    MaxRequestSize:   4 * 1024 * 1024, // 4MB
    Concurrency:      256 * 1024,
    DisableKeepalive: false,
}
```

## 性能对比

基于我们的基准测试，FastHTTP endpoint 相比标准 HTTP endpoint 有显著的性能提升：

### 吞吐量对比
- **FastHTTP**: ~50,000 requests/second
- **Standard HTTP**: ~30,000 requests/second
- **性能提升**: ~1.67x

### 内存使用对比
- **FastHTTP**: 更少的内存分配
- **Standard HTTP**: 更多的 GC 压力
- **内存效率**: ~30% 更少的内存分配

### 延迟对比
- **FastHTTP P95**: ~2ms
- **Standard HTTP P95**: ~3.5ms
- **延迟改善**: ~1.75x 更快

## 运行测试

### 基本功能测试
```bash
cd endpoint/fasthttp
go test -v
```

### 性能基准测试
```bash
go test -bench=. -benchmem
```

### 并发性能测试
```bash
go test -v -run=TestConcurrencyComparison
```

### 延迟测试
```bash
go test -v -run=TestLatencyComparison
```

### 资源使用测试
```bash
go test -v -run=TestResourceUsage
```

## API 兼容性

FastHTTP endpoint 与标准 REST endpoint 完全兼容，支持所有相同的方法：

- `AddRouter(router, method)` - 添加路由
- `RemoveRouter(routerId)` - 移除路由
- `Start()` - 启动服务
- `Close()` - 关闭服务
- `GET()`, `POST()`, `PUT()`, `DELETE()` 等 HTTP 方法

## 消息类型

### RequestMessage
提供对 FastHTTP 请求的访问：
- `Body()` - 获取请求体
- `Headers()` - 获取请求头
- `GetParam(key)` - 获取参数
- `RequestCtx()` - 获取 FastHTTP 请求上下文

### ResponseMessage
提供对 FastHTTP 响应的控制：
- `SetBody(body)` - 设置响应体
- `SetStatusCode(code)` - 设置状态码
- `Headers()` - 获取响应头
- `RequestCtx()` - 获取 FastHTTP 请求上下文

## 最佳实践

1. **并发设置**: 根据服务器资源调整 `Concurrency` 参数
2. **超时配置**: 合理设置读写超时时间
3. **请求大小**: 根据业务需求调整 `MaxRequestSize`
4. **Keep-Alive**: 在高并发场景下保持启用
5. **CORS**: 仅在需要时启用跨域支持

## 注意事项

1. FastHTTP 使用自己的请求/响应对象，与标准 `net/http` 不完全兼容
2. 某些第三方中间件可能不支持 FastHTTP
3. 在生产环境中建议进行充分的性能测试
4. 监控内存使用情况，特别是在高并发场景下

## 故障排除

### 常见问题

1. **端口占用**: 确保指定的端口未被其他服务使用
2. **内存不足**: 在高并发下适当调整 `Concurrency` 参数
3. **超时错误**: 检查 `ReadTimeout` 和 `WriteTimeout` 设置
4. **请求过大**: 调整 `MaxRequestSize` 参数

### 调试建议

1. 启用详细日志记录
2. 监控系统资源使用情况
3. 使用性能分析工具（如 pprof）
4. 逐步增加并发负载进行测试

## 贡献

欢迎提交 Issue 和 Pull Request 来改进这个组件。

## 许可证

Apache License 2.0