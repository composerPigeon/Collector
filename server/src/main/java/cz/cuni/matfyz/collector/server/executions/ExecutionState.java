package cz.cuni.matfyz.collector.server.executions;

/**
 * Enum for representing Executions state in queue
 */
public enum ExecutionState {
    Waiting,
    Running,
    Processed,
    NotFound
}
