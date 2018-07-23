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
        logger.info("New: Will try to greet " + name + " ...");
        HelloRequest request = HelloRequest.newBuilder().setName(name).build();
        HelloReply response;
        try {
            response = blockingStub.sayHello(request);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Greeting returned: " + response.getMessage());
    }

    /**
     * Submit ShuffleInfo to Gaia Controller, use "user:job:map:reduce" as the key for Map<String , FlowInfo>
     * put "TaskAttemptID, IP" in Map<String, String >
     */

    public void submitShuffleInfo(String username, String jobID, Map<String, String> mappersIP, Map<String, String> reducersIP, Map<String, FlowInfo> filenameToFlowsMap) {
        logger.info("NEW! Try to submit ShuffleInfo to controller");
        System.out.println("NEW!!! Submitting ShuffleInfo!!!");

        ShuffleInfo.Builder sinfoBuiler = ShuffleInfo.newBuilder();
        sinfoBuiler.setJobID(jobID).setUsername(username);


        for (Map.Entry<String, FlowInfo> fe : filenameToFlowsMap.entrySet()) {


            ShuffleInfo.FlowInfo.Builder tmpFlow = ShuffleInfo.FlowInfo.newBuilder()
                    .setDataFilename(fe.getValue().getDataFilename())
                    .setMapAttemptID(fe.getValue().getMapAttemptID())
                    .setReduceAttemptID(fe.getValue().getReduceAttemptID())
                    .setStartOffSet(fe.getValue().getStartOffset())
                    .setFlowSize(fe.getValue().getShuffleSize_byte());

            if (fe.getValue().getMapIP() != null) {
                tmpFlow.setMapperIP(fe.getValue().getMapIP());
            } else {
                if (mappersIP.containsKey(fe.getValue().getMapAttemptID())) {
                    tmpFlow.setMapperIP(mappersIP.get(fe.getValue().getMapAttemptID()));
                }
                else {
                    logger.info("no mapIP!");
//                    throw (new Exception("no map IP"));
                }
            }

            if (fe.getValue().getReduceIP() != null) {
                tmpFlow.setReducerIP(fe.getValue().getReduceIP());
            } else {
                if (reducersIP.containsKey(fe.getValue().getReduceAttemptID())) {
                    tmpFlow.setReducerIP(reducersIP.get(fe.getValue().getReduceAttemptID()));
                }
                else {
                    logger.info("no reduceIP!");
//                    throw (new Exception("no map IP"));
                }
            }

            sinfoBuiler.addFlows(tmpFlow);
        }


/*        // Here the Map/Reduce ID are all AttemptID
        // If submitted the ID-IP map, check FlowInfo against the map
        for (Map.Entry<String, String> me : mappersIP.entrySet()) {

            if (!filenameToFlowsMap.containsKey(me.getKey())) {
                sinfoBuiler.
//                filenameToFlowsMap.get()
            }


*//*            sinfoBuiler.addMappers(ShuffleInfo.MapperInfo.newBuilder()
                    .setMapperID(me.getKey())
                    .setMapperIP(me.getValue()));*//*
        }

        for (Map.Entry<String, String> re : reducersIP.entrySet()) {


*//*            sinfoBuiler.addReducers(ShuffleInfo.ReducerInfo.newBuilder()
                    .setReducerID(re.getKey())
                    .setReducerIP(re.getValue()));*//*
        }*/

        // Handle reply
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
//            gaiaClient.greet("x");

            Map<String, String> mappersIP = new HashMap<String, String>();
            Map<String, String> reducersIP = new HashMap<String, String>();
//            TaskInfo taskInfo = new TaskInfo("taskID", "attemptID");
            mappersIP.put("M1", "maxi1");
//            TaskInfo taskInfor = new TaskInfo("taskIDr", "attemptIDr");
            reducersIP.put("R1", "maxi2");

            FlowInfo flowInfo = new FlowInfo("M1", "R1", "/tmp/file/output/ddd/file.out", 0, 500, "maxi1", "maxi2");

            Map<String, FlowInfo> fmap = new HashMap<String, FlowInfo>();
            fmap.put("user:job:map:reduce", flowInfo);

            gaiaClient.submitShuffleInfo("x", "y", mappersIP, reducersIP, fmap);
        } finally {
            gaiaClient.shutdown();
        }
    }
}
