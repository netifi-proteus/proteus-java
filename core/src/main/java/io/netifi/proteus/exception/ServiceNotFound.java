package io.netifi.proteus.exception;

public class ServiceNotFound extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public ServiceNotFound(int namespaceId, int serviceId) {
    super("can not find service for namespace id " + namespaceId + " and service id " + serviceId);
  }
}
