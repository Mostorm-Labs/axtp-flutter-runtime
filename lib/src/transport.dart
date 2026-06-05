import 'dart:async';

import 'model.dart';

abstract class ByteSink {
  void onBytes(Bytes bytes);
}

abstract class AxtpTransport {
  TransportProfile get profile;

  void bind(ByteSink sink);

  FutureOr<void> open();

  FutureOr<void> close();

  void sendBytes(Bytes bytes);
}
