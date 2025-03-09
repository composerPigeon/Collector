package cz.cuni.matfyz.collector.server.executions;

import java.sql.Timestamp;

/**
 * Class representing record from queue
 */
public record Execution(
        String uuid,
        Timestamp added,
        boolean isRunning,
        String instanceName,
        String query
) {}
