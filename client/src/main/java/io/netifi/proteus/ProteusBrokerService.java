package io.netifi.proteus;

import io.netifi.proteus.rsocket.ProteusSocket;

interface ProteusBrokerService {
    ProteusSocket destination(String destination, String group);
    ProteusSocket group(String group);
    ProteusSocket broadcast(String group);
    ProteusSocket service(String service);
    ProteusSocket service(String service, String group);
    ProteusSocket service(String service, String group, String destination);
    ProteusSocket broadcastService(String service);
    ProteusSocket broadcastService(String service, String group);
    void addService(ProteusService service);
}
