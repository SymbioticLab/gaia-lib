/*
 * Copyright 2015, gRPC Authors All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.umich.gaialib;

import io.grpc.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.umich.gaialib.gaiaprotos.*;
import io.grpc.stub.StreamObserver;


/**
 * A simple client that requests a greeting from the {@link HelloWorldServer}.
 */
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

  /**
   * Greet server. If provided, the first element of {@code args} is the name to use in the
   * greeting.
   */
  public static void main(String[] args) throws Exception {
    GaiaClient client = new GaiaClient("localhost", 50051);
    try {
      /* Access a service running on the local machine on port 50051 */
      String user = "world";
      if (args.length > 0) {
        user = args[0]; /* Use the arg as the name to greet if provided */
      }
      client.greet(user);
    } finally {
      client.shutdown();
    }
  }

  /**
   * Server that manages startup/shutdown of a {@code Greeter} server.
   */
  public static class HelloWorldServer {
    private static final Logger logger = Logger.getLogger(HelloWorldServer.class.getName());

    private Server server;

    private void start() throws IOException {
      /* The port on which the server should run */
      int port = 50051;
      server = ServerBuilder.forPort(port)
          .addService(new GreeterImpl())
          .build()
          .start();
      logger.info("Server started, listening on " + port);
      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          // Use stderr here since the logger may have been reset by its JVM shutdown hook.
          System.err.println("*** shutting down gRPC server since JVM is shutting down");
          HelloWorldServer.this.stop();
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

    /**
     * Main launches the server from the command line.
     */
    public static void main(String[] args) throws IOException, InterruptedException {
      final HelloWorldServer server = new HelloWorldServer();
      server.start();
      server.blockUntilShutdown();
    }

    static class GreeterImpl extends GaiaShuffleGrpc.GaiaShuffleImplBase {

      @Override
      public void submitShuffleInfo(ShuffleInfo req, StreamObserver<ShuffleInfoReply> responseObserver) {
        ShuffleInfoReply reply = ShuffleInfoReply.newBuilder().setMessage("Hello " + req.getName()).build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
      }
    }
  }
}
