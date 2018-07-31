package edu.umich.gaialib;

import edu.umich.gaialib.gaiaprotos.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

public abstract class GaiaAbstractServer {
    private static final Logger logger = Logger.getLogger(GaiaServer.class.getName());

    private Server server;
    private int port = 50001;

    public GaiaAbstractServer(int port) {
        this.port = port;
    }

    public void start() throws IOException {
        /* The port on which the server should run */
        int port = 50051;
        server = ServerBuilder.forPort(port)
                .addService(new ShuffleServiceImpl())
                .build()
                .start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                GaiaAbstractServer.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    public void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    class ShuffleServiceImpl extends GaiaShuffleGrpc.GaiaShuffleImplBase {
        @Override
        public void sayHello(HelloRequest req, StreamObserver<HelloReply> responseObserver) {
            HelloReply reply = HelloReply.newBuilder().setMessage("Hello " + req.getName()).build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }

        @Override
        public void submitShuffleInfo(ShuffleInfo req, StreamObserver<ShuffleInfoReply> responseObserver) {

            logger.info("Received single ShuffleInfo, size: " + req.getSerializedSize());
            // do the job abstractly
            processReq(req.getUsername(), req.getJobID(), req.getFlowsList());

            ShuffleInfoReply reply = ShuffleInfoReply.newBuilder().setMessage("Request Complete").build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }

        public StreamObserver<ShuffleInfo> submitShuffleInfoStream(final StreamObserver<ShuffleInfoReply> responseObserver) {

            return new StreamObserver<ShuffleInfo>() {
                String username = null;
                String jobID = null;
                List<ShuffleInfo.FlowInfo> flowsList = new LinkedList<ShuffleInfo.FlowInfo>();

                public void onNext(ShuffleInfo req) {
                    int size = req.getFlowsCount();
                    logger.info("Received ShuffleInfoStream MSG, flows: " + size + " size: " + req.getSerializedSize());

                    if (username == null) username = req.getUsername();
                    if (jobID == null) jobID = req.getJobID();
                    if (size > 0) flowsList.addAll(req.getFlowsList());
                }

                public void onError(Throwable throwable) {
                    logger.warning("Error in submitShuffleInfoStream");
                    throwable.printStackTrace();
                }

                public void onCompleted() {

                    if (flowsList.size() > 0) {
                        processReq(username, jobID, flowsList);
                    } else {
                        logger.info("Shuffle Request has no flowInfo, skipping");
                    }

                    ShuffleInfoReply reply = ShuffleInfoReply.newBuilder().setMessage("Request Complete").build();
                    responseObserver.onNext(reply);
                    responseObserver.onCompleted();
                }
            };
        }

    }

    protected abstract void processReq(String username, String jobID, List<ShuffleInfo.FlowInfo> flowsList);

//    public abstract void processReq(ShuffleInfo req);

}
