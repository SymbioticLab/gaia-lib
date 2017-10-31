package edu.umich.gaialib;


public class FlowInfo {

    String mapAttemptID;
    String reduceAttemptID;
    String dataFilename;

    long startOffset;
    long shuffleSize_byte;

    public FlowInfo(String mapAttemptID, String reduceAttemptID, String dataFilename, long startOffset, long shuffleSize_byte) {
        this.mapAttemptID = mapAttemptID;
        this.reduceAttemptID = reduceAttemptID;
        this.dataFilename = dataFilename;
        this.startOffset = startOffset;
        this.shuffleSize_byte = shuffleSize_byte;
    }

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
