// Gaia client resides in YARN's Application Master
// Application master submits shuffle info to Gaia controller, by invoking the submitShuffleInfo

package edu.umich.gaialib;

import io.grpc.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.umich.gaialib.gaiaprotos.*;


public class GaiaClient {
  private static final Logger logger = Logger.getLogger(GaiaClient.class.getName());

  private final ManagedChannel channel;
  private final GaiaShuffleGrpc.GaiaShuffleBlockingStub blockingStub;

  /** Construct client connecting to HelloWorld server at {@code host:port}. */
  public GaiaClient(String host, int port) {
    this(ManagedChannelBuilder.forAddress(host, port)
        // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
        // needing certificates.
        .usePlaintext(true)
        .build());
  }

  /** Construct client for accessing RouteGuide server using the existing channel. */
  GaiaClient(ManagedChannel channel) {
    this.channel = channel;
    blockingStub = GaiaShuffleGrpc.newBlockingStub(channel);
  }

  public void shutdown() throws InterruptedException {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }

  /** Say hello to server. */
  public void greet(String name) {
    logger.info("Will try to greet " + name + " ...");
    ShuffleInfo request = ShuffleInfo.newBuilder().setName(name).build();
    ShuffleInfoReply response;
    try {
      response = blockingStub.submitShuffleInfo(request);
    } catch (StatusRuntimeException e) {
      logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
      return;
    }
    logger.info("Greeting: " + response.getMessage());
  }
}
