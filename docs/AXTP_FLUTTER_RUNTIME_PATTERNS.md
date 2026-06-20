# AXTP Flutter Runtime 架构与设计模式

Flutter runtime 复用 C++ runtime 的固定分层：

```text
AxtpTransport <-> AxtpEndpoint -> AxtpCore -> BasicBroker
```

`axtp-flutter-runtime` 是独立维护的纯 Dart 包，可被 Flutter app 直接依赖，也可在命令行用 Dart 测试。平台 I/O（HID、BLE、USB、TCP、WebSocket）不进入 core；它们应作为 Flutter plugin 或应用层 connector 实现 `AxtpTransport`。

## Target 映射

| 区域 | 代码位置 | 职责 |
|---|---|---|
| model | `lib/src/model.dart` | payload/meta/profile/core event value object |
| wire | `lib/src/wire.dart` | FramedBinary、WebSocketJsonRpc、CRC、payload codec |
| broker | `lib/src/broker.dart` | dynamic handler dispatch 和结果队列 |
| core | `lib/src/core.dart` | 协议状态、pending response、event/outbound 队列 |
| endpoint | `lib/src/endpoint.dart` | transport、core、broker 的唯一 glue layer |
| client | `lib/src/client.dart` | Flutter 友好的 dynamic RPC wrapper |
| generated | `lib/src/generated/axtp_registry_generated.dart` | Generator 输出的 ID、registry、lookup helper |

## P0 能力

- FramedBinary：12B standard frame header、Big-Endian / network byte order fields、CRC16-CCITT-FALSE、message fragmentation/reassembly。
- WebSocketJsonRpc：每次 `onBytes()` 输入一条完整 UTF-8 text message，支持 `sid/op/d` request/event/response。
- Dynamic RPC first：`callJson`、`callTlv`、`callRawBytes` 和 broker raw/json/tlv handler。
- Mock transport：用于 Flutter 单元测试、demo 和本地业务 handler 验证。
- Generator 集成：runtime 仓库自己的 `devtools/generators/` 会刷新 Flutter generated registry。

## 边界约束

- `AxtpCore` 不创建、持有或 import 平台 transport/plugin。
- `BasicBroker` 不知道 frame/header/CRC，也不反向调用 core。
- `AxtpEndpoint` 是唯一 glue layer，负责 poll 顺序：core events -> broker tasks -> broker results -> outbound flush。
- Typed domain API 只能作为 dynamic/raw RPC 之上的便利层，不能成为 runtime routing 前提。
- WebSocketJsonRpc 是正式 AXTP wire mode，不是 legacy adapter。

## Flutter 扩展方式

新增平台连接时只实现 `AxtpTransport`：

1. `profile` 填写 `TransportKind`、`AxtpWireMode`、message/text/binary 能力和 `preferredFrameSize`。
2. `bind(ByteSink)` 保存 runtime sink，并在平台收到数据时调用 `sink.onBytes(bytes)`。
3. `sendBytes(bytes)` 只处理平台写入和 message/report 边界，不解析 AXTP payload。
4. 连接生命周期、权限、重连和 UI 状态留在 app/plugin 层。

推荐验证：

```bash
cd /path/to/mostormlabs/axtp-flutter-runtime
dart analyze lib test tool
dart test
dart tool/smoke.dart
```
