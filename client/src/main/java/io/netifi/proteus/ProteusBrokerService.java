package io.netifi.proteus;

import io.netifi.proteus.rs.ProteusSocket;

interface ProteusBrokerService {
    ProteusSocket destination(String destination, String group);
    ProteusSocket group(String group);
}
