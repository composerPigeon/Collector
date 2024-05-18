package cz.cuni.matfyz.collector.server.exceptions;

/**
 * Exception thrown from Execution manager when some problem appear during operations with executions
 */
public class ExecutionManagerException extends Exception {
    public ExecutionManagerException(String msg, Throwable cause) { super(msg, cause);}
}
