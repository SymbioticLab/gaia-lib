// Gaia client resides in YARN's Application Master
// Application master submits shuffle info to Gaia controller, by invoking the submitShuffleInfo

package edu.umich.gaialib;

import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.*;

import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import edu.umich.gaialib.gaiaprotos.*;

public class TerraClient {
    private static final Logger logger = Logger.getLogger(GaiaClient.class.getName());

    private final ManagedChannel channel;
    private final GaiaShuffleGrpc.GaiaShuffleFutureStub futureStub;

    /**
     * Construct client connecting to Gaia Controller server at {@code host:port}.
     */
    public TerraClient(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port)
                // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
                // needing certificates.
                .usePlaintext(true)
                .build());
    }

    /**
     * Construct client for accessing Gaia Controller server using the existing channel.
     */
    TerraClient(ManagedChannel channel) {
        this.channel = channel;
        futureStub = GaiaShuffleGrpc.newFutureStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    /**
     * Say hello to server.
     */
    public ListenableFuture<HelloReply> greet(String name) {
        logger.info("TerraClient.greet called!!!");
        HelloRequest request = HelloRequest.newBuilder().setName(name).build();
        ListenableFuture<HelloReply> helloReplyFuture =
                        futureStub.sayHello(request);
        // vill variable go out of scope?
        return helloReplyFuture;
    }
}
