package edu.umich.gaialib;

import edu.umich.gaialib.gaiaprotos.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * Server that manages startup/shutdown of a {@code Greeter} server.
 */
public class GaiaServer {
    private static final Logger logger = Logger.getLogger(GaiaServer.class.getName());

    private Server server;

    private void start() throws IOException {
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
                GaiaServer.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    static class ShuffleServiceImpl extends GaiaShuffleGrpc.GaiaShuffleImplBase {
        @Override
        public void sayHello(HelloRequest req, StreamObserver<HelloReply> responseObserver) {
            HelloReply reply = HelloReply.newBuilder().setMessage("Hello " + req.getName()).build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }

        @Override
        public void submitShuffleInfo(ShuffleInfo req, StreamObserver<ShuffleInfoReply> responseObserver) {
            /**
             * do the job here
             */

            System.out.println(req.getUsername());
            System.out.println(req.getJobID());
//            System.out.println(req.getMappers(0).getMapperIP());
//            System.out.println(req.getMappers(0).getMapperID());
//            System.out.println(req.getReducers(0).getReducerIP());
//            System.out.println(req.getReducers(0).getReducerID());

            ShuffleInfoReply reply = ShuffleInfoReply.newBuilder().setMessage("Hello " + req.getUsername()).build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        GaiaServer gaiaServer = new GaiaServer();
        gaiaServer.start();
        gaiaServer.blockUntilShutdown();
    }
}
