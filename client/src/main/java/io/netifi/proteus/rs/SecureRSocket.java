package io.netifi.proteus.rs;

import io.netifi.proteus.auth.SessionUtil;
import io.rsocket.RSocket;
import java.util.concurrent.atomic.AtomicLong;
import reactor.core.publisher.Mono;

/** RSocket implement that provides session and token information to secure a request */
public interface SecureRSocket extends RSocket {

  SessionUtil getSessionUtil();

  Mono<AtomicLong> getCurrentSessionCounter();

  Mono<byte[]> getCurrentSessionToken();
}
