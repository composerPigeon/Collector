package cz.cuni.matfyz.collector.server.exceptions;

import cz.cuni.matfyz.collector.server.configurationproperties.SystemType;

/**
 * Class with all error messages that are used on server to report errors to user
 */
public abstract class ErrorMessages {

    public static String setExecutionRunningErrorMsg(String uuid) {
        return "Error setting execution '" + uuid + "' as running.";
    }
    public static String queueInsertExecutionErrorMsg(String instanceName, String query) {
        return "Insert of Execution(instanceName: " + instanceName + ", query: " + query + ") to queue failed.";
    }
    public static String findExecutionStateErrorMsg(String uuid) {
        return "Error finding execution '" + uuid + "' state in queue.";
    }
    public static String getExecutionResultErrorMsg(String uuid) {
        return "Error getting execution '" + uuid + "' result from persistor.";
    }
    public static String getExecutionsFromQueueErrorMsg() {
        return "Error getting waiting executions from queue.";
    }
    public static String removeExecutionErrorMsg(String uuid) {
        return "Error removing execution '" + uuid + "' from queue.";
    }

    public static String saveExecutionResultErrorMsg(String uuid) {
        return "Error saving execution '" + uuid + "' result.";
    }

    public static String saveExecutionErrorErrorMsg(String uuid) {
        return "Error saving execution '" + uuid + "' error.";
    }
    public static String unexpectedErrorMsg() {
        return "Unexpected error occurred.";
    }

    public static String unexpectedErrorDuringExecutionErrorMsg(String uuid) {
        return "Unexpected error occurred while processing of execution '" + uuid + "'.";
    }

    public static String badCreateRequestErrorMsg() {
        return "Server expect POST's request body to be in json format. Json object has to contain fields 'instance' and 'query'.";
    }

    public static String serializeWrappersErrorMsg() {
        return "Error serializing wrappers.";
    }

    public static String nonExistentWrapper(String uuid, String instanceName) {
        return "Execution '" + uuid + "' is trying to execute query through non-existent instance '" + instanceName + "'.";
    }

    public static String nonExistentExecution(String uuid) {
        return "Execution '" + uuid + "' does not exist.";
    }

    public static String driverForQueueNotFound() {
        return "Execution queue cannot be initialized because driver 'org.h2.Driver' was not found.";
    }

    public static String queueFailedToInitialize() {
        return "Execution queue cannot be initialized because it failed to connect to database.";
    }

    public static String missingWrapperInitializer(SystemType type) {
        return "Wrapper initializer for system type '" + type + "' is missing.";
    }
}
