package io.netifi.proteus.vizceral.service;

import com.google.protobuf.Empty;
import io.netifi.proteus.Proteus;
import io.netifi.proteus.vizceral.VizceralServiceClient;
import io.netty.buffer.Unpooled;
import java.util.Optional;
import java.util.function.Supplier;

public class VizceralSupplier implements Supplier<Vizceral> {
  private final Proteus proteus;
  private final String group;

  public VizceralSupplier(Proteus proteus, Optional<String> group) {
    this.proteus = proteus;
    this.group = group.orElse("com.netifi.proteus.vizceral");
  }

  @Override
  public Vizceral get() {
    VizceralServiceClient vizceralClient = new VizceralServiceClient(proteus.group(group));
    return () -> vizceralClient.visualisations(Empty.getDefaultInstance(), Unpooled.EMPTY_BUFFER);
  }
}
