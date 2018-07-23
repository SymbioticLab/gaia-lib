package edu.umich.gaialib;

// Information of Map/Reduce Task

public class TaskInfo {

    final private String TaskID;
    final private String AttemptID;

    public TaskInfo(String taskID, String attemptID) {
        TaskID = taskID;
        AttemptID = attemptID;
    }

    public String getTaskID() {
        return TaskID;
    }

    public String getAttemptID() {
        return AttemptID;
    }

    @Override
    public String toString() {
        return TaskID + "_" + AttemptID;
    }
}
