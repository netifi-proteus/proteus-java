package io.netifi.proteus.auth.exception;

/** */
public class AuthenticationDeniedException extends RuntimeException {
  @Override
  public synchronized Throwable fillInStackTrace() {
    return this;
  }
}
