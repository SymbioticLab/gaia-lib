// Gaia client resides in YARN's Application Master
// Application master submits shuffle info to Gaia controller, by invoking the submitShuffleInfo

package edu.umich.gaialib;

import io.grpc.*;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.umich.gaialib.gaiaprotos.*;

public class GaiaClient {
    private static final Logger logger = Logger.getLogger(GaiaClient.class.getName());

    private final ManagedChannel channel;
    private final GaiaShuffleGrpc.GaiaShuffleBlockingStub blockingStub;

    /**
     * Construct client connecting to Gaia Controller server at {@code host:port}.
     */
    public GaiaClient(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port)
                // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
                // needing certificates.
                .usePlaintext(true)
                .build());
    }

    /**
     * Construct client for accessing Gaia Controller server using the existing channel.
     */
    GaiaClient(ManagedChannel channel) {
        this.channel = channel;
        blockingStub = GaiaShuffleGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    /**
     * Say hello to server.
     */
    public void greet(String name) {
        logger.info("Will try to greet " + name + " ...");
        ShuffleInfo request = ShuffleInfo.newBuilder().setUsername(name).build();
        ShuffleInfoReply response;
        try {
            response = blockingStub.submitShuffleInfo(request);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Greeting: " + response.getMessage());
    }

    /**
     * Submit ShuffleInfo to Gaia Controller
     */
    public void submitShuffleInfo(String username, String jobID, Map<String, String> mappersIP, Map<String, String> reducersIP) {
        logger.info("Try to submit ShuffleInfo to controller");

        ShuffleInfo.Builder sinfoBuiler = ShuffleInfo.newBuilder();
        sinfoBuiler.setJobID(jobID).setUsername(username);

        for (Map.Entry<String, String> me : mappersIP.entrySet()) {
            sinfoBuiler.addMappers( ShuffleInfo.MapperInfo.newBuilder().setMapperID(me.getKey()).setMapperIP(me.getValue()));
        }

        for (Map.Entry<String, String> re : reducersIP.entrySet()) {
            sinfoBuiler.addReducers( ShuffleInfo.ReducerInfo.newBuilder().setReducerID(re.getKey()).setReducerIP(re.getValue()));
        }

        ShuffleInfoReply response;

        try {
            response = blockingStub.submitShuffleInfo(sinfoBuiler.build());
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }

        logger.info("Gaia controller returned: " + response.getMessage());
    }
}
