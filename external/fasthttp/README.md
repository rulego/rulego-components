# FastHTTP API Call Node

基于 FastHTTP 的高性能 HTTP 客户端组件，支持代理配置和流式响应。可以无感替换标准 `restApiCall` 客户端组件，提供更高效的 HTTP 请求处理能力。

## 功能特性

- 🚀 **高性能**: 基于 FastHTTP 库，性能优于标准 HTTP 库
- 🔗 **代理支持**: 支持 HTTP/HTTPS 和 SOCKS5 代理
- 🔐 **认证支持**: 支持代理用户名密码认证
- 🌐 **系统代理**: 支持使用系统代理配置
- 📡 **流式响应**: 支持 Server-Sent Events (SSE) 流式数据处理
- 🔒 **TLS 配置**: 支持 TLS 证书验证配置
- ⚡ **连接池**: 支持连接池配置，提高并发性能
- 📝 **模板支持**: URL、Headers、Body 支持模板变量替换

## 配置参数

### 基础配置

| 参数                         | 类型                | 默认值                                  | 说明                         |
|----------------------------|-------------------|--------------------------------------|----------------------------|
| `restEndpointUrlPattern`   | string            | -                                    | HTTP URL地址，支持模板变量          |
| `requestMethod`            | string            | POST                                 | 请求方法 (GET/POST/PUT/DELETE) |
| `withoutRequestBody`       | bool              | false                                | 是否不发送请求体                   |
| `headers`                  | map[string]string | {"Content-Type": "application/json"} | 请求头，支持模板变量                 |
| `body`                     | string            | -                                    | 请求体，支持模板变量                 |
| `readTimeoutMs`            | int               | 2000                                 | 读取超时时间（毫秒）                 |
| `insecureSkipVerify`       | bool              | false                                | 是否跳过 TLS 证书验证              |
| `maxParallelRequestsCount` | int               | 200                                  | 最大并发连接数                    |

### 代理配置

| 参数                         | 类型     | 默认值   | 说明                       |
|----------------------------|--------|-------|--------------------------|
| `enableProxy`              | bool   | false | 是否启用代理                   |
| `useSystemProxyProperties` | bool   | false | 是否使用系统代理配置               |
| `proxyScheme`              | string | -     | 代理协议 (http/https/socks5) |
| `proxyHost`                | string | -     | 代理服务器地址                  |
| `proxyPort`                | int    | -     | 代理服务器端口                  |
| `proxyUser`                | string | -     | 代理用户名                    |
| `proxyPassword`            | string | -     | 代理密码                     |

## 使用示例

### 基础 HTTP 请求

```json
{
  "id": "fasthttp1",
  "type": "fasthttpApiCall",
  "name": "HTTP请求",
  "configuration": {
    "restEndpointUrlPattern": "https://api.example.com/data",
    "requestMethod": "POST",
    "headers": {
      "Content-Type": "application/json",
      "Authorization": "Bearer ${metadata.token}"
    },
    "body": "{\"name\":\"${msg.name}\",\"value\":\"${msg.value}\"}",
    "readTimeoutMs": 5000
  }
}
```

### HTTP 代理配置

```json
{
  "id": "fasthttp2",
  "type": "fasthttpApiCall",
  "name": "HTTP代理请求",
  "configuration": {
    "restEndpointUrlPattern": "https://api.example.com/data",
    "requestMethod": "GET",
    "enableProxy": true,
    "proxyScheme": "http",
    "proxyHost": "proxy.example.com",
    "proxyPort": 8080,
    "proxyUser": "username",
    "proxyPassword": "password"
  }
}
```

### SOCKS5 代理配置

```json
{
  "id": "fasthttp3",
  "type": "fasthttpApiCall",
  "name": "SOCKS5代理请求",
  "configuration": {
    "restEndpointUrlPattern": "https://api.example.com/data",
    "requestMethod": "POST",
    "enableProxy": true,
    "proxyScheme": "socks5",
    "proxyHost": "127.0.0.1",
    "proxyPort": 1080
  }
}
```

### 系统代理配置

```json
{
  "id": "fasthttp4",
  "type": "fasthttpApiCall",
  "name": "系统代理请求",
  "configuration": {
    "restEndpointUrlPattern": "https://api.example.com/data",
    "requestMethod": "GET",
    "enableProxy": true,
    "useSystemProxyProperties": true
  }
}
```

### Server-Sent Events (SSE) 流式响应

```json
{
  "id": "fasthttp5",
  "type": "fasthttpApiCall",
  "name": "SSE流式请求",
  "configuration": {
    "restEndpointUrlPattern": "https://api.example.com/stream",
    "requestMethod": "GET",
    "headers": {
      "Accept": "text/event-stream",
      "Cache-Control": "no-cache"
    }
  }
}
```

## 模板变量

支持在以下字段中使用模板变量：

- `restEndpointUrlPattern`: URL 地址
- `headers`: 请求头的键和值
- `body`: 请求体内容

### 变量格式

- `${metadata.key}`: 从消息元数据中获取值
- `${msg.key}`: 从消息负载中获取值

### 示例

```json
{
  "restEndpointUrlPattern": "https://api.example.com/users/${metadata.userId}/posts",
  "headers": {
    "Authorization": "Bearer ${metadata.token}",
    "X-Request-ID": "${metadata.requestId}"
  },
  "body": "{\"title\":\"${msg.title}\",\"content\":\"${msg.content}\"}"
}
```

## 响应处理

### 成功响应

- HTTP 状态码为 200 时，消息发送到 `Success` 链
- 响应内容设置为消息数据
- 元数据中包含：
  - `status`: HTTP 状态文本
  - `statusCode`: HTTP 状态码

### 失败响应

- HTTP 状态码非 200 时，消息发送到 `Failure` 链
- 元数据中包含：
  - `status`: HTTP 状态文本
  - `statusCode`: HTTP 状态码
  - `errorBody`: 错误响应内容

### SSE 流式响应

对于 SSE 流式响应，每个事件都会触发一次消息处理：

- 元数据中包含 `eventType`: 事件类型
- 消息数据为事件内容

## 代理配置说明

### 系统代理

当启用 `useSystemProxyProperties` 时，组件会自动读取以下环境变量：

- `HTTP_PROXY` 或 `http_proxy`
- `HTTPS_PROXY` 或 `https_proxy`

### 代理协议支持

- **HTTP/HTTPS**: 使用 HTTP CONNECT 方法建立隧道
- **SOCKS5**: 使用 SOCKS5 协议进行连接

### 代理认证

支持用户名密码认证，适用于 HTTP 和 SOCKS5 代理。

### TLS 配置

可以设置 `insecureSkipVerify: true` 跳过证书验证。

## 错误处理

组件会处理以下类型的错误：

1. **网络连接错误**: 无法连接到目标服务器
2. **代理连接错误**: 无法连接到代理服务器
3. **HTTP 错误**: 服务器返回错误状态码
4. **超时错误**: 请求超时
5. **模板解析错误**: 模板变量解析失败

所有错误都会将消息发送到 `Failure` 链，并在元数据中记录错误信息。