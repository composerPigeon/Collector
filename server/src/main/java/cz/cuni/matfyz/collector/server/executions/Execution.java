package cz.cuni.matfyz.collector.server.executions;

/**
 * Class representing record from queue
 */
public record Execution(
        String uuid,
        long count,
        boolean isRunning,
        String instanceName,
        String query
) {}
