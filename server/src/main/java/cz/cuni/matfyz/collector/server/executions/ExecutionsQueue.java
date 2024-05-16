package cz.cuni.matfyz.collector.server.executions;

import cz.cuni.matfyz.collector.server.exceptions.ErrorMessages;
import cz.cuni.matfyz.collector.server.exceptions.QueueExecutionsException;
import cz.cuni.matfyz.collector.wrappers.exceptions.QueryExecutionException;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Component
public class ExecutionsQueue {
    private String _connectionString;
    private String _userName;
    private String _password;
    private long _count;

    private void _initDatabase() throws QueueExecutionsException {
        try (Statement statement = DriverManager.getConnection(_connectionString, _userName, _password).createStatement()) {
            String createTableQuery = """
                    CREATE TABLE IF NOT EXISTS executions (
                        uuid VARCHAR(36),
                        count BIGINT UNIQUE,
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
    @PostConstruct
    public void init() throws QueueExecutionsException {
        try {
            _connectionString = "jdbc:h2:mem:test";
            _userName = "";
            _password = "";
            _count = Long.MIN_VALUE;

            Class.forName ("org.h2.Driver");
            _initDatabase();
        } catch (ClassNotFoundException e) {
            String errMsg = "H2 driver 'org.h2.Driver' is missing.";
            throw new QueueExecutionsException(e);
        }

    }

    public String createExecution(String instanceName, String query) throws QueueExecutionsException {
        try (Statement statement = DriverManager.getConnection(_connectionString, _userName, _password).createStatement()) {
            String uuid = UUID.randomUUID().toString();

            String insertQuery = "INSERT INTO executions VALUES ( '" + uuid + "', " + _count + ", false, '" + instanceName + "', '" + query + "' );";
            statement.executeUpdate(insertQuery);
            _count += 1;
            return uuid;
        } catch (SQLException e) {
            throw new QueueExecutionsException(e);
        }
    }

    public ExecutionState getExecutionState(String uuid) throws QueueExecutionsException {
        try (Statement statement = DriverManager.getConnection(_connectionString, _userName, _password).createStatement()) {
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

    public void setRunning(String uuid) throws QueueExecutionsException {
        try (Statement statement = DriverManager.getConnection(_connectionString, _userName, _password).createStatement()) {
            statement.executeUpdate("UPDATE executions SET isrunning = true WHERE uuid = '" + uuid + "' ;");
        } catch (SQLException e) {
            throw new QueueExecutionsException(e);
        }
    }

    public List<Execution> getExecutions() throws QueueExecutionsException {
        try (Statement statement = DriverManager.getConnection(_connectionString, _userName, _password).createStatement()) {
            ResultSet result = statement.executeQuery("SELECT * FROM executions WHERE isrunning = false ORDER BY count ASC;");
            List<Execution> executions = new ArrayList<>();

            while (result.next()) {
                executions.add(new Execution(
                        result.getString("uuid"),
                        result.getLong("count"),
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

    public void removeExecution(String uuid) throws QueueExecutionsException {
        try (Statement statement = DriverManager.getConnection(_connectionString, _userName, _password).createStatement()){
            statement.executeUpdate("DELETE FROM executions WHERE uuid = '" + uuid + "' ;");
        } catch (SQLException e) {
            throw new QueueExecutionsException(e);
        }
    }
}