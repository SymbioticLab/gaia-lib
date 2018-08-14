// Gaia client resides in YARN's Application Master
// Application master submits shuffle info to Gaia controller, by invoking the submitShuffleInfo

package edu.umich.gaialib;

import io.grpc.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.umich.gaialib.gaiaprotos.*;
import io.grpc.stub.StreamObserver;

public class TerraClient {
    private static final Logger logger = Logger.getLogger(TerraClient.class.getName());
    private static final int MAX_FLOW_PER_MSG = 5000;

    private final ManagedChannel channel;
    private final GaiaShuffleGrpc.GaiaShuffleFutureStub futureStub;
    private final GaiaShuffleGrpc.GaiaShuffleStub asyncStub;

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
        asyncStub = GaiaShuffleGrpc.newStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    /*
     */
/**
 * Say hello to server.
 *//*

    @Deprecated
    public TerraFutureComplex<HelloReply> greet(String name) {
        logger.info("New: Will try to greet " + name + " ...");
        HelloRequest request = HelloRequest.newBuilder().setName(name).build();
        com.google.common.util.concurrent.ListenableFuture<edu.umich.gaialib.gaiaprotos.HelloReply> response;
        try {
            response = futureStub.sayHello(request);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return null;
        }
        return new TerraFutureComplex<HelloReply>(response);
    }
*/

    /**
     * Submit ShuffleInfo to Gaia Controller, use "user:job:map:reduce" as the key for Map<String , FlowInfo>
     * put "TaskAttemptID, IP" in Map<String, String >
     * Change filenameToFlowsMap into List<Map.Entry<String, FlowInfo>>
     */
    public TerraFuture<ShuffleInfoReply> submitShuffleInfo(String username, String jobID, Map<String, String> mappersIP,
                                                           Map<String, String> reducersIP, List<FlowInfo> flowInfoList) {
        logger.info("Try to submit ShuffleInfo to controller");
//        System.out.println("NEW!!! Submitting ShuffleInfo!!!");

        ArrayList<ShuffleInfo> shuffleInfos = new ArrayList<ShuffleInfo>();
        ShuffleInfo.Builder sinfoBuiler = ShuffleInfo.newBuilder();
        sinfoBuiler.setJobID(jobID).setUsername(username);

        for (FlowInfo fe : flowInfoList) {


            ShuffleInfo.FlowInfo.Builder tmpFlowInfo = ShuffleInfo.FlowInfo.newBuilder()
                    .setDataFilename(fe.getDataFilename())
                    .setMapAttemptID(fe.getMapAttemptID())
                    .setReduceAttemptID(fe.getReduceAttemptID())
                    .setStartOffSet(fe.getStartOffset())
                    .setFlowSize(fe.getShuffleSize_byte());

            if (fe.getMapIP() != null) {
                tmpFlowInfo.setMapperIP(fe.getMapIP());
            } else {
                if (mappersIP.containsKey(fe.getMapAttemptID())) {
                    tmpFlowInfo.setMapperIP(mappersIP.get(fe.getMapAttemptID()));
                } else {
                    logger.warning("no mapIP!");
//                    throw (new Exception("no map IP"));
                }
            }

            if (fe.getReduceIP() != null) {
                tmpFlowInfo.setReducerIP(fe.getReduceIP());
            } else {
                if (reducersIP.containsKey(fe.getReduceAttemptID())) {
                    tmpFlowInfo.setReducerIP(reducersIP.get(fe.getReduceAttemptID()));
                } else {
                    logger.warning("no reduceIP!");
//                    throw (new Exception("no map IP"));
                }
            }

            if (sinfoBuiler.getFlowsCount() >= MAX_FLOW_PER_MSG) {
                // If the msg if full, add to buffer and create a new msg
                shuffleInfos.add(sinfoBuiler.build());
                sinfoBuiler = ShuffleInfo.newBuilder();
                sinfoBuiler.setJobID(jobID).setUsername(username);

            } else {
                sinfoBuiler.addFlows(tmpFlowInfo);
            }
        }

        // Add the last msg
        shuffleInfos.add(sinfoBuiler.build());

        return submitShuffleInfos(shuffleInfos);

    }

    /**
     * Submit ShuffleInfo to Gaia Controller, use "user:job:map:reduce" as the key for Map<String , FlowInfo>
     * put "TaskAttemptID, IP" in Map<String, String >
     */
    @Deprecated
    public TerraFuture<ShuffleInfoReply> submitShuffleInfo(String username, String jobID, Map<String, String> mappersIP,
                                                           Map<String, String> reducersIP, Map<String, FlowInfo> filenameToFlowsMap) {
        logger.info("Try to submit ShuffleInfo to controller flowInfos = {}" + filenameToFlowsMap.size());
//        System.out.println("NEW!!! Submitting ShuffleInfo!!!");

        ArrayList<ShuffleInfo> shuffleInfos = new ArrayList<ShuffleInfo>();
        ShuffleInfo.Builder sinfoBuiler = ShuffleInfo.newBuilder();
        sinfoBuiler.setJobID(jobID).setUsername(username);


        for (Map.Entry<String, FlowInfo> fe : filenameToFlowsMap.entrySet()) {


            ShuffleInfo.FlowInfo.Builder tmpFlowInfo = ShuffleInfo.FlowInfo.newBuilder()
                    .setDataFilename(fe.getValue().getDataFilename())
                    .setMapAttemptID(fe.getValue().getMapAttemptID())
                    .setReduceAttemptID(fe.getValue().getReduceAttemptID())
                    .setStartOffSet(fe.getValue().getStartOffset())
                    .setFlowSize(fe.getValue().getShuffleSize_byte());

            if (fe.getValue().getMapIP() != null) {
                tmpFlowInfo.setMapperIP(fe.getValue().getMapIP());
            } else {
                if (mappersIP.containsKey(fe.getValue().getMapAttemptID())) {
                    tmpFlowInfo.setMapperIP(mappersIP.get(fe.getValue().getMapAttemptID()));
                } else {
                    logger.warning("no mapIP!");
//                    throw (new Exception("no map IP"));
                }
            }

            if (fe.getValue().getReduceIP() != null) {
                tmpFlowInfo.setReducerIP(fe.getValue().getReduceIP());
            } else {
                if (reducersIP.containsKey(fe.getValue().getReduceAttemptID())) {
                    tmpFlowInfo.setReducerIP(reducersIP.get(fe.getValue().getReduceAttemptID()));
                } else {
                    logger.warning("no reduceIP!");
//                    throw (new Exception("no map IP"));
                }
            }

            if (sinfoBuiler.getFlowsCount() >= MAX_FLOW_PER_MSG) {
                // If the msg if full, add to buffer and create a new msg
                shuffleInfos.add(sinfoBuiler.build());
                sinfoBuiler = ShuffleInfo.newBuilder();
                sinfoBuiler.setJobID(jobID).setUsername(username);

            } else {
                sinfoBuiler.addFlows(tmpFlowInfo);
            }
        }

        // Add the last msg
        shuffleInfos.add(sinfoBuiler.build());

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

        return submitShuffleInfos(shuffleInfos);

//
//        // Single req log
//        com.google.common.util.concurrent.ListenableFuture<edu.umich.gaialib.gaiaprotos.ShuffleInfoReply> response;
//
//        try {
//            response = futureStub.submitShuffleInfo(sinfoBuiler.build());
//        } catch (StatusRuntimeException e) {
//            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
//            return null;
//        }
//
//        return new TerraFutureComplex<ShuffleInfoReply>(response);
    }

    private TerraFuture<ShuffleInfoReply> submitShuffleInfos(ArrayList<ShuffleInfo> shuffleInfos) {

        final TerraFuture<ShuffleInfoReply> future = new TerraFuture<ShuffleInfoReply>();

        StreamObserver<ShuffleInfoReply> responseObserver = new StreamObserver<ShuffleInfoReply>() {
            public void onNext(ShuffleInfoReply shuffleInfoReply) {

            }

            public void onError(Throwable throwable) {
                logger.warning("ERROR in submitShuffleInfos");
                throwable.printStackTrace();
            }

            public void onCompleted() {
                logger.info("Finished shuffleInfoStreamRPC");
                future.setDone(true);
            }
        };

        StreamObserver<ShuffleInfo> requestObserver = asyncStub.submitShuffleInfoStream(responseObserver);

        try {
            for (ShuffleInfo si : shuffleInfos) {
                requestObserver.onNext(si);
            }
        } catch (RuntimeException e) {
            requestObserver.onError(e);
            e.printStackTrace();
        }
        requestObserver.onCompleted();

        return future;
//        return new TerraFutureComplex<ShuffleInfoReply>();
    }

    public static void main(String[] args) throws Exception {
        TerraClient terraClient = new TerraClient("localhost", 50051);
        try {
//            terraClient.greet("x");

            Map<String, String> mappersIP = new HashMap<String, String>();
            Map<String, String> reducersIP = new HashMap<String, String>();
//            TaskInfo taskInfo = new TaskInfo("taskID", "attemptID");
            mappersIP.put("M1", "maxi1");
//            TaskInfo taskInfor = new TaskInfo("taskIDr", "attemptIDr");
            reducersIP.put("R1", "maxi2");

            FlowInfo flowInfo = new FlowInfo("M1", "R1", "/tmp/file/output/ddd/file.out", 0, 500, "maxi1", "maxi2");

            Map<String, FlowInfo> fmap = new HashMap<String, FlowInfo>();
            fmap.put("user:job:map:reduce", flowInfo);

            terraClient.submitShuffleInfo("x", "y", mappersIP, reducersIP, fmap);
        } finally {
            terraClient.shutdown();
        }
    }
}
