package cz.cuni.matfyz.collector.server.executions;

import cz.cuni.matfyz.collector.server.exceptions.QueueExecutionsException;
import jakarta.annotation.PostConstruct;
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
    private Connection _connection;

    /**
     * Init method that initialize database in such way, that it will create executions table inside the database
     * @throws QueueExecutionsException when some SQLException during table initialization occur
     */
    private void _initDatabase() throws QueueExecutionsException {
        try (Statement statement =  _connection.createStatement()) {
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

            statement.executeUpdate(createTableQuery);
        } catch (SQLException e) {
            throw new QueueExecutionsException(e);
        }
    }

    /**
     * Init method that initialize database
     * @throws QueueExecutionsException when some org.h2.Driver is not found
     */
    @PostConstruct
    public void init() throws QueueExecutionsException {
        try {
            String connectionString = "jdbc:h2:mem:test";
            String userName = "";
            String password = "";

            Class.forName ("org.h2.Driver");
            _connection = DriverManager.getConnection(connectionString, userName, password);
            _initDatabase();
        } catch (ClassNotFoundException e) {
            String errMsg = "H2 driver 'org.h2.Driver' is missing.";
            throw new QueueExecutionsException(errMsg, e);
        } catch (SQLException e) {
            throw new QueueExecutionsException("Error initialising of H2 connection", e);
        }

    }

    /**
     * Method for creating execution id and inserting it into queue
     * @param instanceName instance name on which query should be executed
     * @param query query to be executed
     * @return execution id of newly inserted query
     * @throws QueueExecutionsException when some SQLException occur during process
     */
    public String createExecution(String instanceName, String query) throws QueueExecutionsException {
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
        catch (SQLException e) {
            throw new QueueExecutionsException(e);
        }
    }

    /**
     * Method for getting execution state from queue
     * @param uuid execution id
     * @return execution state gathered from queue
     * @throws QueueExecutionsException when some SQLException occur during process
     */
    public ExecutionState getExecutionState(String uuid) throws QueueExecutionsException {
        try (Statement statement = _connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("SELECT isrunning FROM executions WHERE uuid = '" + uuid + "' ;");

            if (resultSet.next()) {
                boolean isRunning = resultSet.getBoolean("isrunning");
                if (isRunning)
                    return ExecutionState.Running;
                else
                    return ExecutionState.Waiting;
            }

            resultSet.close();
            return ExecutionState.NotFound;
        } catch (SQLException e) {
            throw new QueueExecutionsException(e);
        }
    }

    /**
     * Method for setting execution to running state
     * @param uuid execution identifier
     * @throws QueueExecutionsException when some SQLException occur during process
     */
    public void setRunning(String uuid) throws QueueExecutionsException {
        try (Statement statement = _connection.createStatement()) {
            statement.executeUpdate("UPDATE executions SET isrunning = true WHERE uuid = '" + uuid + "' ;");
        } catch (SQLException e) {
            throw new QueueExecutionsException(e);
        }
    }

    /**
     * Method for getting list of waiting executions from queue to be executed by scheduler
     * @return list of executions
     * @throws QueueExecutionsException when some SQLException occur during process
     */
    public List<Execution> getExecutions() throws QueueExecutionsException {
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

            result.close();
            return executions;
        } catch (SQLException e) {
            throw new QueueExecutionsException(e);
        }
    }

    /**
     * Method for removing execution from queu after it's query result was saved
     * @param uuid execution identifier
     * @throws QueueExecutionsException when some SQLException occur during process
     */
    public void removeExecution(String uuid) throws QueueExecutionsException {
        try (Statement statement = _connection.createStatement()){
            statement.executeUpdate("DELETE FROM executions WHERE uuid = '" + uuid + "' ;");
        } catch (SQLException e) {
            throw new QueueExecutionsException(e);
        }
    }

    @Override
    public void close() throws Exception {
        _connection.close();
    }
}
