package cz.cuni.matfyz.collector.server.exceptions;

import cz.cuni.matfyz.collector.server.configurationproperties.SystemType;
import org.springframework.stereotype.Component;

/**
 * Class with all error messages that are used on server to report errors to user
 */
@Component
public class ErrorMessages {

    public String setExecutionRunningErrorMsg(String uuid, Throwable cause) {
        return "Can't set execution '" + uuid + "' as running because: " + cause.getMessage();
    }
    public String queueInsertExecutionErrorMsg(String instanceName, String query, Throwable cause) {
        return "Insert of Execution(instanceName: " + instanceName + ", query: " + query + ") to queue failed because: " + cause.getMessage();
    }
    public String findExecutionStateErrorMsg(String uuid, Throwable cause) {
        return "Can't find execution '" + uuid + "' state in queue because: " + cause.getMessage();
    }
    public String getExecutionResultErrorMsg(String uuid, Throwable cause) {
        return "Can't get execution '" + uuid + "' result from persistor because: " + cause.getMessage();
    }
    public String getExecutionsFromQueueErrorMsg(Throwable cause) {
        return "Can't get waiting executions from queue because: " + cause.getMessage();
    }
    public String removeExecutionErrorMsg(String uuid, Throwable cause) {
        return "Can't remove execution '" + uuid + "' from queue because: " + cause.getMessage();
    }

    public String saveExecutionResultErrorMsg(String uuid, Throwable cause) {
        return "Can't save execution '" + uuid + "' result because: " + cause.getMessage();
    }

    public String saveExecutionErrorErrorMsg(String uuid, Throwable cause) {
        return "Can't save execution '" + uuid + "' error message because: " + cause.getMessage();
    }
    public String unexpectedErrorMsg() {
        return "Unexpected error occurred.";
    }

    public String badCreateRequestErrorMsg() {
        return "Server expect POST's request body to be in json format. Json object has to contain fields 'instance' and 'query'.";
    }

    public String nonExistentWrapper(String uuid, String instanceName) {
        return "Execution '" + uuid + "' is trying to execute query through non-existent instance '" + instanceName + "'.";
    }

    public String nonExistentExecution(String uuid) {
        return "Execution '" + uuid + "' does not exist.";
    }

    public String driverForQueueNotFound() {
        return "Execution queue cannot be initialized because driver 'org.h2.Driver' was not found.";
    }

    public String queueFailedToInitialize() {
        return "Execution queue cannot be initialized because it failed to connect to database.";
    }

    public String missingWrapperInitializer(SystemType type) {
        return "Wrapper initializer for system type '" + type + "' is missing.";
    }

    public String missingPersistorInitializer(SystemType type) {
        return "Persistor initializer for system type '" + type + "' is missing.";
    }
}
