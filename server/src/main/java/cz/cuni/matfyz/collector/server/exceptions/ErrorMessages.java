package cz.cuni.matfyz.collector.server.exceptions;

import cz.cuni.matfyz.collector.server.configurationproperties.SystemType;
import cz.cuni.matfyz.collector.server.executions.Execution;
import org.springframework.stereotype.Component;

/**
 * Class with all error messages that are used on server to report errors to user
 */
@Component
public class ErrorMessages {

    public String setExecutionRunningErrorMsg(String uuid, Throwable cause) {
        return format(
                "Can't set execution '%s' as running, cause { %s }",
                uuid,
                cause.getMessage()
        );
    }
    public String queueInsertExecutionErrorMsg(String instanceName, String query, Throwable cause) {
        return format(
                "Insert of execution for instance '%s' and query '%s' to queue failed, cause { %s }",
                instanceName,
                query,
                cause.getMessage()
        );
    }
    public String findExecutionStateErrorMsg(String uuid, Throwable cause) {
        return format(
                "Can't find execution '%s' state in queue, cause { %s }",
                uuid,
                cause.getMessage()
        );
    }
    public String getExecutionResultErrorMsg(String uuid, Throwable cause) {
        return format(
                "Can't get execution '%s' record from persistor, cause { %s }",
                uuid,
                cause.getMessage()
        );
    }
    public String getExecutionsFromQueueErrorMsg(Throwable cause) {
        return format(
                "Can't get waiting executions from queue, cause { %s }",
                cause.getMessage()
        );
    }
    public String removeExecutionErrorMsg(String uuid, Throwable cause) {
        return format(
                "Can't remove execution '%s' from queue, cause { %s }",
                uuid,
                cause.getMessage()
        );
    }

    public String saveExecutionResultErrorMsg(String uuid, Throwable cause) {
        return format(
                "Can't save execution '%s' result, cause { %s }",
                uuid,
                cause.getMessage()
        );
    }

    public String saveExecutionErrorErrorMsg(String uuid, Throwable cause) {
        return format(
                "Can't save execution '%s' error message, cause { %s }",
                uuid,
                cause.getMessage()
        );
    }
    public String unexpectedErrorMsg()
    {
        return format("Unexpected error occurred");
    }

    public String badCreateRequestErrorMsg() {
        return format("Invalid request. Server expect POST's request body to be in json format. Object must have two string fields 'instanceName' and 'query'");
    }

    public String nonExistentWrapper(String uuid, String instanceName) {
        return format(
                "Instance '%s' does not exist and therefore execution '%s' failed",
                instanceName,
                uuid
        );
    }

    public String nonExistentExecution(String uuid) {
        return format("Execution '%s' does not exist", uuid);
    }

    public String driverForQueueNotFound() {
        return format("Execution queue cannot be initialized because driver 'org.h2.Driver' was not found.");
    }

    public String queueFailedToInitialize() {
        return format("Execution queue cannot be initialized becase it failed to connect to h2 database");
    }

    public String missingWrapperInitializer(SystemType type) {
        return format(
                "Wrapper initializer for system '%s' is missing",
                type.name().toLowerCase()
        );
    }

    public String missingPersistorInitializer(SystemType type) {
        return format(
                "Persistor initializer for system '%s' is missing",
                type.name().toLowerCase()
        );
    }

    public String executionOfWrapperFailed(Execution execution, Throwable cause) {
        return format(
                "Running execution '%s' with query '%s' on instance '%s' failed, cause { %s }",
                execution.uuid(),
                execution.query(),
                execution.instanceName(),
                cause.getMessage()
        );
    }

    private String format(String content, Object... args) {
        return String.format(content, args);
    }
}
