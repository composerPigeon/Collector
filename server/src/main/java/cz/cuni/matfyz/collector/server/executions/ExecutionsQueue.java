package cz.cuni.matfyz.collector.server.executions;

import cz.cuni.matfyz.collector.server.exceptions.ErrorMessages;
import cz.cuni.matfyz.collector.server.exceptions.QueueExecutionsException;
import org.springframework.stereotype.Component;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Class representing queue of executions before they are evaluated and their results saved. It is implemented with in memory H2 database
 */
@Component
public class ExecutionsQueue implements AutoCloseable {
    private final Connection _connection;
    private final ErrorMessages _errors;

    public ExecutionsQueue(ErrorMessages errorMessages) throws QueueExecutionsException {
        _connection = new ConnectionBuilder()
                .setConnectionString("jdbc:h2:mem:test")
                .build();
        _errors = errorMessages;
    }

    /**
     * Method for creating execution id and inserting it into queue
     * @param instanceName instance name on which query should be executed
     * @param query query to be executed
     * @return execution id of newly inserted query
     * @throws SQLException when some error occur during process
     */
    public String createExecution(String instanceName, String query) throws SQLException {
        String insertQuery = "INSERT INTO executions(uuid, added, isrunning, instance, query) VALUES (?, ?, ?, ?, ?);";
        try (PreparedStatement statement = _connection.prepareStatement(insertQuery)) {
            String uuid = UUID.randomUUID().toString();
            statement.setString(1, uuid);
            statement.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
            statement.setBoolean(3, false);
            statement.setString(4, instanceName);
            statement.setString(5, query);
            statement.executeUpdate();
            return uuid;
        }
    }

    /**
     * Method for getting execution state from queue
     * @param uuid execution id
     * @return execution state gathered from queue
     * @throws SQLException when some SQLException occur during process
     */
    public ExecutionState getExecutionState(String uuid) throws SQLException {
        try (Statement statement = _connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("SELECT isrunning FROM executions WHERE uuid = '" + uuid + "' ;");

            if (resultSet.next()) {
                boolean isRunning = resultSet.getBoolean("isrunning");
                if (isRunning)
                    return ExecutionState.Running;
                else
                    return ExecutionState.Waiting;
            }
            return ExecutionState.NotFound;
        }
    }

    /**
     * Method for setting execution to running state
     * @param uuid execution identifier
     * @throws SQLException when some SQLException occur during process
     */
    public void setRunning(String uuid) throws SQLException {
        try (Statement statement = _connection.createStatement()) {
            statement.executeUpdate("UPDATE executions SET isrunning = true WHERE uuid = '" + uuid + "' ;");
        }
    }

    /**
     * Method for getting list of waiting executions from queue to be executed by scheduler
     * @return list of executions
     * @throws SQLException when some SQLException occur during process
     */
    public List<Execution> getWaitingExecutions() throws SQLException {
        try (Statement statement = _connection.createStatement()) {
            ResultSet result = statement.executeQuery("SELECT * FROM executions WHERE isrunning = false ORDER BY added ASC;");
            List<Execution> executions = new ArrayList<>();

            while (result.next()) {
                executions.add(new Execution(
                        result.getString("uuid"),
                        result.getTimestamp("added"),
                        result.getBoolean("isrunning"),
                        result.getString("instance"),
                        result.getString("query")
                ));
            }

            return executions;
        }
    }

    /**
     * Method for removing execution from queue after it's query result was saved
     * @param uuid execution identifier
     * @throws SQLException when some SQLException occur during process
     */
    public void removeExecution(String uuid) throws SQLException {
        try (Statement statement = _connection.createStatement()){
            statement.executeUpdate("DELETE FROM executions WHERE uuid = '" + uuid + "' ;");
        }
    }

    @Override
    public void close() throws Exception {
        _connection.close();
    }

    private class ConnectionBuilder {
        private Connection _connection;
        private String _connectionString;
        private String _userName;
        private String _password;

        public ConnectionBuilder() {
            _userName = "";
            _password = "";
            _connectionString = "";
            _connection = null;
        }

        public ConnectionBuilder setConnectionString(String connectionString) {
            _connectionString = connectionString;
            return this;
        }

        public ConnectionBuilder setUserName(String userName) {
            _userName = userName;
            return this;
        }

        public ConnectionBuilder setPassword(String password) {
            _password = password;
            return this;
        }

        private void _initializeConnection() throws SQLException, ClassNotFoundException {
            Class.forName ("org.h2.Driver");
            _connection = DriverManager.getConnection(_connectionString, _userName, _password);
        }

        private void _createExecutionTable() throws SQLException {
            String createTableQuery = """
                CREATE TABLE IF NOT EXISTS executions (
                    uuid VARCHAR(36),
                    added TIMESTAMP NOT NULL,
                    isrunning BOOLEAN NOT NULL,
                    instance VARCHAR(20) NOT NULL,
                    query VARCHAR(150) NOT NULL,
                    PRIMARY KEY (uuid)
                )
                """;

            try (Statement statement =  _connection.createStatement()) {
                statement.executeUpdate(createTableQuery);
            }
        }

        public Connection build() throws QueueExecutionsException {
            try {
                _initializeConnection();
                _createExecutionTable();
                return _connection;
            } catch (ClassNotFoundException e) {
                throw new QueueExecutionsException(
                        _errors.driverForQueueNotFound(),
                        e
                );
            } catch (SQLException e) {
                throw new QueueExecutionsException(
                        _errors.queueFailedToInitialize(),
                        e
                );
            }
        }

    }
}
