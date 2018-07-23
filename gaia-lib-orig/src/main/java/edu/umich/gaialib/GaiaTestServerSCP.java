package edu.umich.gaialib;

import edu.umich.gaialib.gaiaprotos.ShuffleInfo;
import sun.misc.IOUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

// A simple unit test for GaiaClient and Hadoop integration

public class GaiaTestServerSCP {
    private static final Logger logger = Logger.getLogger(GaiaTestServerSCP.class.getName());
    private static TestServer testServer;

    static class TestServer extends GaiaAbstractServer {

        public TestServer(int port) {
            super(port);
        }

        public void processReq(String username, String jobID, List<ShuffleInfo.FlowInfo> flowsList) {
            logger.info("Received req: " + username + jobID);

/*            try {
                System.out.println("Finished scp, Blocked");
                int inChar = System.in.read();
                System.out.print("Now proceeding");
            }
            catch (IOException e){
                System.out.println("Error reading from user");
            }*/

            List<String> cmdList = new ArrayList<String>();

            for (int i = 0; i < flowsList.size(); i++) {

                ShuffleInfo.FlowInfo finfo = flowsList.get(i);
                String dataName = finfo.getDataFilename();
                String trimmedDirPath = dataName.substring(0, dataName.lastIndexOf("/"));
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

                String cmd_mkdir = "ssh jimmyyou@" + dstIP + " mkdir -p " + trimmedDirPath;

                String ccmd = cmd_mkdir + "; scp " + srcIP + ":" + dataName + " " + dstIP + ":" + trimmedDirPath;
//                String ccmd = cmd_mkdir + " ; rsync -avr " + srcIP + ":" + trimmedDirPath + "/ " + dstIP + ":" + trimmedDirPath;

//                System.out.println("Invoking " + cmd_mkdir);

                if (srcIP.equals(dstIP)) {
                    logger.info("Ignoring Co-located " + dataName);
                    continue;
                }

                long flowVolume = finfo.getFlowSize();
                if (flowVolume == 0) {
                    flowVolume = 1;
                    // FIXME Terra now ignores flowVol = 0 flows
                    logger.info("Ignoring size=0 flow " + finfo.getDataFilename());
                    continue;
                }

                // Filter index files
                if (dataName.endsWith("index")) {
                    logger.info("Got an index file " + dataName);
//                    indexFileFGs.put(fgID, fg);
                }

                cmdList.add(ccmd);

                /*Process p = null;
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
                        System.err.println(line);
                        int inChar = System.in.read();
                    }

                    p = Runtime.getRuntime().exec(cmd);
                    p.waitFor();

                    bri = new BufferedReader(new InputStreamReader(p.getInputStream()));
                    while ((line = bri.readLine()) != null) {
                        System.out.println(line);
                    }

                    bri = new BufferedReader(new InputStreamReader(p.getErrorStream()));
                    while ((line = bri.readLine()) != null) {
                        System.err.println(line);
                        int inChar = System.in.read();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } */
            }


            int pNum = Runtime.getRuntime().availableProcessors();
            logger.info("My JAVA Runtime procs: " + pNum);
            List<Process> pool = new ArrayList<Process>();

            for (String cmd : cmdList) {
                Process p = null;
                try {

                    p = Runtime.getRuntime().exec(cmd);
                    pool.add(p);
                    System.out.println("Invoking " + cmd);

/*                    String line;
                    BufferedReader bri = new BufferedReader(new InputStreamReader(p.getInputStream()));
                    while ((line = bri.readLine()) != null) {
                        System.out.println(line);
                    }

                    bri = new BufferedReader(new InputStreamReader(p.getErrorStream()));
                    while ((line = bri.readLine()) != null) {
                        System.err.println(line);
                    }*/
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            // Wait for all cmd to finish
            logger.info("Waiting for SCP file transfer");
            for (Process p : pool) {
                try {
                    p.waitFor();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            logger.info("SCP file transfer finished");

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
