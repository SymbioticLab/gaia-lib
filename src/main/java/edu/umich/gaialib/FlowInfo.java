package edu.umich.gaialib;


public class FlowInfo {

    String mapID;
    String reduceID;
    String dataFilename;

    long shuffleSize_byte;

    public FlowInfo(String mapID, String reduceID, String dataFilename, long shuffleSize_byte) {
        this.mapID = mapID;
        this.reduceID = reduceID;
        this.dataFilename = dataFilename;
        this.shuffleSize_byte = shuffleSize_byte;
    }

    public String getMapID() {
        return mapID;
    }

    public String getReduceID() {
        return reduceID;
    }

    public String getDataFilename() {
        return dataFilename;
    }

    public long getShuffleSize_byte() {
        return shuffleSize_byte;
    }

    @Override
    public String toString() {
        return "FlowInfo{" +
                "mapID='" + mapID + '\'' +
                ", reduceID='" + reduceID + '\'' +
                ", dataFilename='" + dataFilename + '\'' +
                ", shuffleSize_byte=" + shuffleSize_byte +
                '}';
    }
}
