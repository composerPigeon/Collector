package cz.cuni.matfyz.collector.server.exceptions;

public abstract class ErrorMessages {

    public static String setExecutionRunningErrorMsg(String uuid) {
        return "Error setting execution " + uuid + " as running.";
    }
    public static String queueInsertExecutionErrorMsg(String instanceName, String query) {
        return "Insert of Execution(instanceName: " + instanceName + ", query: " + query + ") to queue failed.";
    }
    public static String findExecutionStateErrorMsg(String uuid) {
        return "Error finding execution " + uuid + " state in queue.";
    }
    public static String getExecutionResultErrorMsg(String uuid) {
        return "Error getting execution " + uuid + " result from persistor.";
    }
    public static String getExecutionsFromQueueErrorMsg() {
        return "Error getting wating executions from queue.";
    }
    public static String removeExecutionErrorMsg(String uuid) {
        return "Error removing execution " + uuid + " from queue.";
    }

    public static String saveExecutionResult(String uuid) {
        return "Error saving execution " + uuid + " result.";
    }

    public static String unexpectedErrorMsg() {
        return "Unexpected error occurred.";
    }

    public static String badCreateRequestErrorMsg() {
        return "Server expect POST's request body to be in json format. Json object has to contain fields 'instance' and 'query'.";
    }

    public static String serializeWrappersErrorMsg() {
        return "Error serializing wrappers.";
    }
}
