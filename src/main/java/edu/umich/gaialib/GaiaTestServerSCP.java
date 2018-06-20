package edu.umich.gaialib;

import edu.umich.gaialib.gaiaprotos.ShuffleInfo;
import sun.misc.IOUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.logging.Logger;

// A simple unit test for GaiaClient and Hadoop integration

public class GaiaTestServerSCP {
    private static final Logger logger = Logger.getLogger(GaiaTestServerSCP.class.getName());
    private static TestServer testServer;

    static class TestServer extends GaiaAbstractServer {

        public TestServer(int port) {
            super(port);
        }

        public void processReq(ShuffleInfo req) {
            logger.info("Received req: " + req);

/*            try {
                System.out.println("Finished scp, Blocked");
                int inChar = System.in.read();
                System.out.print("Now proceeding");
            }
            catch (IOException e){
                System.out.println("Error reading from user");
            }*/

            for ( int i = 0 ; i < req.getFlowsList().size(); i ++){

                ShuffleInfo.FlowInfo finfo = req.getFlowsList().get(i);

                String dataName = finfo.getDataFilename();

                String trimmedDirPath = dataName.substring( 0 , dataName.lastIndexOf("/"));
                // trimmedDirPath = trimmedDirPath.substring(0 , trimmedDirPath.lastIndexOf("/"));

                // trim to only include the /output

                String srcID = finfo.getMapAttemptID();
                String dstID = finfo.getReduceAttemptID();

                String srcIP = finfo.getMapperIP();
                String dstIP = finfo.getReducerIP();

//                for (ShuffleInfo.MapperInfo minfo : req.getMappersList()){
//                    if (minfo.getMapperID().equals(srcID)){
//                        srcIP = minfo.getMapperIP().split(":")[0];
//                    }
//                }
//
//                for (ShuffleInfo.ReducerInfo rinfo : req.getReducersList()){
//                    if (rinfo.getReducerID().equals(dstID)){
//                        dstIP = rinfo.getReducerIP().split(":")[0];
//                    }
//                }

//                String dstIP = req.getReducersList().get(i).getReducerIP().split(":",2)[0];
//                String srcIP = req.getMappersList().get(i).getMapperIP().split(":",2)[0];

//                String cmd_mkdir = "ssh wentingt@" + dstIP + " 'mkdir -p " + trimmedData + "'";
                String cmd_mkdir = "ssh wentingt@" + dstIP + " mkdir -p " + trimmedDirPath ;

                String cmd = "scp -r " + srcIP + ":" + trimmedDirPath + "/* " + dstIP + ":" + trimmedDirPath;

                System.out.println("Invoking " + cmd_mkdir);
                System.out.println("Invoking " + cmd);

                Process p = null;
                try {

                    p = Runtime.getRuntime().exec(cmd_mkdir);
                    p.waitFor();

                    String line;
                    BufferedReader bri = new BufferedReader(new InputStreamReader(p.getInputStream()));
                    while ((line = bri.readLine()) != null) {
                        System.out.println(line);
                    }

                    bri = new BufferedReader(new InputStreamReader(p.getErrorStream()));
                    while ((line = bri.readLine()) != null) {
                        System.out.println(line);
                    }

                    p = Runtime.getRuntime().exec(cmd);
                    p.waitFor();

                    bri = new BufferedReader(new InputStreamReader(p.getInputStream()));
                    while ((line = bri.readLine()) != null) {
                        System.out.println(line);
                    }

                    bri = new BufferedReader(new InputStreamReader(p.getErrorStream()));
                    while ((line = bri.readLine()) != null) {
                        System.out.println(line);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

/*            try {
                System.out.println("Finished scp, Blocked");
                int inChar = System.in.read();
                System.out.print("Now proceeding");
            }
            catch (IOException e){
                System.out.println("Error reading from user");
            }*/

        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {

        testServer = new TestServer(50051);

        testServer.start();
        testServer.blockUntilShutdown();
    }
}
