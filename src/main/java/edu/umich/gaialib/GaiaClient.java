// Gaia client resides in YARN's Application Master
// Application master submits shuffle info to Gaia controller, by invoking the submitShuffleInfo

package edu.umich.gaialib;

import io.grpc.*;

import java.util.HashMap;
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
        HelloRequest request = HelloRequest.newBuilder().setName(name).build();
        HelloReply response;
        try {
            response = blockingStub.sayHello(request);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Greeting: " + response.getMessage());
    }

    /**
     * Submit ShuffleInfo to Gaia Controller
     */

    public void submitShuffleInfo(String username, String jobID, Map<TaskInfo, String> mappersIP, Map<TaskInfo, String> reducersIP, Map<String , FlowInfo> filenameToFlowsMap) {
        logger.info("Try to submit ShuffleInfo to controller");

        ShuffleInfo.Builder sinfoBuiler = ShuffleInfo.newBuilder();
        sinfoBuiler.setJobID(jobID).setUsername(username);


        // TODO: also set Attempt ID
        for (Map.Entry<TaskInfo, String> me : mappersIP.entrySet()) {
            sinfoBuiler.addMappers( ShuffleInfo.MapperInfo.newBuilder()
                    .setMapperID(me.getKey().getTaskID())
                    .setMapperIP(me.getValue()));
        }

        for (Map.Entry<TaskInfo, String> re : reducersIP.entrySet()) {
            sinfoBuiler.addReducers( ShuffleInfo.ReducerInfo.newBuilder()
                    .setReducerID(re.getKey().getTaskID())
                    .setReducerIP(re.getValue()));
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

    public static void main(String[] args) throws Exception {
        GaiaClient gaiaClient = new GaiaClient("localhost", 50051);
        try {
            gaiaClient.greet("x");

            Map<TaskInfo, String> mappersIP = new HashMap<TaskInfo, String>();
            Map<TaskInfo, String> reducersIP = new HashMap<TaskInfo, String>();
            TaskInfo taskInfo = new TaskInfo("taskID", "attemptID");
            mappersIP.put(taskInfo, "http");
            TaskInfo taskInfor = new TaskInfo("taskIDr", "attemptIDr");
            reducersIP.put(taskInfor, "httpr");

            FlowInfo flowInfo = new FlowInfo("mapID", "reduceID", "dir/file.out", 100);

            Map<String, FlowInfo> fmap = new HashMap<String, FlowInfo>();
            fmap.put( "dir/file.out", flowInfo);

            gaiaClient.submitShuffleInfo("x", "y", mappersIP, reducersIP, fmap);
        } finally {
            gaiaClient.shutdown();
        }
    }
}
