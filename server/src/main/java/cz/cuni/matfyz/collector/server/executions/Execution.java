package cz.cuni.matfyz.collector.server.executions;

public record Execution(
        String uuid,
        long count,
        boolean isRunning,
        String instanceName,
        String query
) {}
