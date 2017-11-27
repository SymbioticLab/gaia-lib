package edu.umich.gaialib;

// Use this class as a middle-box between YARN and Gaia, now there is no dependency issue.

public class FlowInfo {

    String mapAttemptID;
    String reduceAttemptID;
    String dataFilename;

    long startOffset;
    long shuffleSize_byte;

    String mapIP;
    String reduceIP;

    public FlowInfo(String mapAttemptID, String reduceAttemptID, String dataFilename, long startOffset, long shuffleSize_byte, String mapIP, String reduceIP) {
        this.mapAttemptID = mapAttemptID;
        this.reduceAttemptID = reduceAttemptID;
        this.dataFilename = dataFilename;
        this.startOffset = startOffset;
        this.shuffleSize_byte = shuffleSize_byte;
        this.mapIP = mapIP;
        this.reduceIP = reduceIP;
    }

/*    public FlowInfo(String mapAttemptID, String reduceAttemptID, String dataFilename, long startOffset, long shuffleSize_byte) {
        this.mapAttemptID = mapAttemptID;
        this.reduceAttemptID = reduceAttemptID;
        this.dataFilename = dataFilename;
        this.startOffset = startOffset;
        this.shuffleSize_byte = shuffleSize_byte;
    }*/

    public String getMapAttemptID() {
        return mapAttemptID;
    }

    public String getReduceAttemptID() {
        return reduceAttemptID;
    }

    public String getDataFilename() {
        return dataFilename;
    }

    public long getShuffleSize_byte() {
        return shuffleSize_byte;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public String getMapIP() {
        return mapIP;
    }

    public String getReduceIP() {
        return reduceIP;
    }

    @Override
    public String toString() {
        return "FlowInfo{" +
                "mapAttemptID='" + mapAttemptID + '\'' +
                ", reduceAttemptID='" + reduceAttemptID + '\'' +
                ", dataFilename='" + dataFilename + '\'' +
                ", startOffset=" + startOffset +
                ", shuffleSize_byte=" + shuffleSize_byte +
                '}';
    }
}
