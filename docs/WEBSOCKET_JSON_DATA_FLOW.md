# AXTP WebSocket JSON RPC 数据流

本文用固定示例说明 WebSocket JSON profile 在各层之间的转换。示例中的
`sid` 固定为 `ABC12345`，实际运行时由服务端生成。

## 先记住一条边界

WebSocket JSON profile 的网络消息是一条完整 JSON 文本消息，不是 framed-binary
格式。`AxtpWebSocketTransport` 与 runtime 之间使用 runtime 定义的
`Bytes`（Dart 中是 `Uint8List`）：

```text
WebSocket 收到 String
    -> utf8.encode(String)
    -> Bytes
    -> AxtpEndpoint._EndpointByteSink
    -> AxtpCore -> InboundProcessor -> JsonRpcDecoder
```

发送方向相反：runtime 的 `JsonRpcEncoder` 先生成 Bytes，transport 再执行
`utf8.decode(bytes)`，把它作为 WebSocket 文本发给服务端。因此服务端看到的
不是 `Uint8List`，而是 runtime 已经包装好的完整 AXTP JSON。

下文的 Bytes 均用 `utf8.encode(<JSON 文本>)` 表示；这就是实际的
`Uint8List` 内容，长度取决于 JSON 文本的 UTF-8 字节数。

## 一、握手：Hello → Identify → Identified

### 1. 服务端发送 Hello（op=0）

网络 JSON：

```json
{"sid":"","op":0,"d":{"axtpVersion":"0.13.0","rpcVersion":1}}
```

transport 交给 runtime 的 Bytes：

```dart
final bytes = utf8.encode(
  '{"sid":"","op":0,"d":{"axtpVersion":"0.13.0","rpcVersion":1}}',
);
// bytes.length == 61
```

`JsonRpcDecoder` 解码为：

```dart
RpcPayload(
  encoding: RpcEncoding.json,
  op: RpcOp.hello,
  requestId: 0,
  methodOrEventId: 0,
  meta: PayloadMeta(
    sourceProtocol: SourceProtocol.jsonRpc,
    jsonSid: '',
  ),
  body: utf8.encode('{"axtpVersion":"0.13.0","rpcVersion":1}'),
)
```

`AxtpClient.ensureAppReady()` 从 runtime 的 session 队列取出这个 payload，
不会把 Hello 当成业务 event 暴露给产品。

### 2. 客户端发送 Identify（op=2）

产品只提供握手参数：`randomSeed=17`、`eventMasks="sports.basketball"`。runtime 生成
完整 AXTP JSON：

```json
{"sid":"","op":2,"d":{"randomSeed":17,"eventMasks":"sports.basketball"}}
```

发送给 transport 的 Bytes：

```dart
final bytes = utf8.encode(
  '{"sid":"","op":2,"d":{"randomSeed":17,"eventMasks":"sports.basketball"}}',
);
// bytes.length == 72
```

服务端收到的就是上面的完整包装消息；服务端不会只收到
`{"randomSeed":17,"eventMasks":"sports.basketball"}`。

runtime 内部发送前的模型是：

```dart
RpcPayload(
  encoding: RpcEncoding.json,
  op: RpcOp.identify,
  meta: PayloadMeta(sourceProtocol: SourceProtocol.jsonRpc, jsonSid: ''),
  body: utf8.encode('{"randomSeed":17,"eventMasks":"sports.basketball"}'),
)
```

### 3. 服务端发送 Identified（op=3）

网络 JSON：

```json
{"sid":"ABC12345","op":3,"d":{"negotiatedRpcVersion":1}}
```

Bytes：

```dart
final bytes = utf8.encode(
  '{"sid":"ABC12345","op":3,"d":{"negotiatedRpcVersion":1}}',
);
// bytes.length == 56
```

runtime 解码后 `RpcPayload.op == RpcOp.identified`，并把 `jsonSid` 保存到
`AxtpClient.sessionSid`。此后三层都知道当前 sid，业务请求会自动注入它。

握手阶段产品一般只能看到：

```text
client.connect() 返回 SdkError.success
client.sessionSid == "ABC12345"
```

如果监听 `client.traces`，还会看到 `hello`、`identify`、`identified` 三条
`ClientTrace/WebSocketTrace` 诊断记录。

## 二、客户端主动 request 和服务端 response

### 1. 产品输入是裸方法名和参数

```dart
final resultText = await client.callJson('cast.getStatus', '{}');
```

产品没有填写 `sid`、`op`、`id`。runtime 生成 request ID `1`，并通过生成注册表
找到 `cast.getStatus` 的 method ID `0x1612`（这个数字留在 runtime 模型中，
WebSocket JSON 的 `method` 字段仍使用方法名）。

### 2. runtime 生成并发送完整 request

runtime 的 `RpcPayload`（发送前）：

```dart
RpcPayload(
  encoding: RpcEncoding.json,
  op: RpcOp.request,
  requestId: 1,
  methodOrEventId: 0x1612,
  meta: PayloadMeta(
    sourceProtocol: SourceProtocol.jsonRpc,
    requestId: 1,
    jsonSid: 'ABC12345',
    jsonMethodOrEventName: 'cast.getStatus',
  ),
  body: utf8.encode('{}'),
)
```

`JsonRpcEncoder` 把它包装成网络 JSON：

```json
{"sid":"ABC12345","op":7,"d":{"id":1,"method":"cast.getStatus","params":{}}}
```

传给 transport 的 Bytes：

```dart
final bytes = utf8.encode(
  '{"sid":"ABC12345","op":7,"d":{"id":1,"method":"cast.getStatus","params":{}}}',
);
// bytes.length == 76
```

`AxtpWebSocketTransport.sendBytes()` 再把这些 Bytes 转回 UTF-8 文本，
`web_socket_client` 将这条文本消息发给服务端。因此服务端收到的是带 `sid`、
`op`、`id`、`method`、`params` 的完整 AXTP 包装，不是裸 `{}`。

### 3. 服务端返回完整 response

本地 `websocket_server_demo` 返回：

```json
{"sid":"ABC12345","op":8,"d":{"id":1,"status":{"ok":true,"code":0},"result":{"healthy":true,"server":"websocket_server_demo","params":{}}}}
```

transport 收到文本后交给 runtime 的 Bytes：

```dart
final bytes = utf8.encode(
  '{"sid":"ABC12345","op":8,"d":{"id":1,"status":{"ok":true,"code":0},"result":{"healthy":true,"server":"websocket_server_demo","params":{}}}}',
);
// bytes.length == 139
```

runtime 解码后的 `RpcPayload` 是：

```dart
RpcPayload(
  encoding: RpcEncoding.json,
  op: RpcOp.requestResponse,
  requestId: 1,
  statusCode: ErrorCode.success,
  meta: PayloadMeta(
    sourceProtocol: SourceProtocol.jsonRpc,
    requestId: 1,
    jsonSid: 'ABC12345',
  ),
  // 注意：这里的 body 只保留 d.result，不再包含外层 sid/op/d/status。
  body: utf8.encode(
    '{"healthy":true,"server":"websocket_server_demo","params":{}}',
  ),
)
```

`AxtpClient.callJson()` 最终返回的字符串是：

```json
{"healthy":true,"server":"websocket_server_demo","params":{}}
```

所以结论是：服务端收发双方在网络上都使用完整 AXTP 包装；只有 runtime
返回给产品的 `callJson()` 结果才剥掉 `sid`、`op`、`d` 等协议外层。完整的
request/response 仍可通过 `WebSocketTrace` 记录诊断。

## 三、服务端主动下发 Event（op=6）

本地 server 在发送 Identified 后主动发送：

```json
{"sid":"ABC12345","op":6,"d":{"event":"cast.sessionStateChanged","data":{"receiverPhase":"idle","sessionState":"idle"}}}
```

transport 交给 runtime 的 Bytes：

```dart
final bytes = utf8.encode(
  '{"sid":"ABC12345","op":6,"d":{"event":"cast.sessionStateChanged","data":{"receiverPhase":"idle","sessionState":"idle"}}}',
);
// bytes.length == 120
```

`JsonRpcDecoder` 查生成的 event registry（`cast.sessionStateChanged` 的 event
ID 是 `0x1602`），生成：

```dart
RpcPayload(
  encoding: RpcEncoding.json,
  op: RpcOp.event,
  requestId: 0,
  methodOrEventId: 0x1602,
  meta: PayloadMeta(
    sourceProtocol: SourceProtocol.jsonRpc,
    jsonSid: 'ABC12345',
    jsonMethodOrEventName: 'cast.sessionStateChanged',
  ),
  body: utf8.encode(
    '{"receiverPhase":"idle","sessionState":"idle"}',
  ),
)
```

事件不会进入默认 broker，也不会被编码后回写给服务端。runtime 通过
`AxtpClient.events` 同步发布这个 `RpcPayload`，WebSocket client 再转换为：

```dart
AxtpWebSocketEvent(
  name: 'cast.sessionStateChanged',
  sid: 'ABC12345',
  data: <String, Object?>{
    'receiverPhase': 'idle',
    'sessionState': 'idle',
  },
  payload: originalRpcPayload,
)
```

同一条事件还会生成诊断模型：

```dart
WebSocketTrace(
  direction: 'in',
  kind: 'event',
  detail: 'ABC12345',
  payload: <String, Object?>{
    'event': 'cast.sessionStateChanged',
    'data': <String, Object?>{
      'receiverPhase': 'idle',
      'sessionState': 'idle',
    },
  },
)
```

最终 `websocket_demo` 监听 `client.events`，生成一条 `DemoLogEntry`，UI 显示
事件名、sid 和 data。完整链路为：

```text
server JSON String
  -> AxtpWebSocketTransport._onMessage
  -> UTF-8 Bytes
  -> _EndpointByteSink.onBytes
  -> AxtpEndpoint.onTransportBytes
  -> AxtpCore.byteSink / InboundProcessor
  -> JsonRpcDecoder
  -> RpcPayload(op=event)
  -> AxtpCore.onRpc
  -> AxtpClient.events
  -> AxtpWebSocketClient._onRuntimeEvent
  -> AxtpWebSocketEvent + WebSocketTrace
  -> websocket_demo controller
  -> DemoLogEntry
  -> UI
```
