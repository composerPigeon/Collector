package cz.cuni.matfyz.collector.server.executions;

import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

@Component
public class ExecutionsQueue {
    private String _connectionString;
    private String _userName;
    private String _password;
    private long _count;

    private void _initDatabase() {
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
            e.printStackTrace();
        }
    }
    @PostConstruct
    public void init() {
        try {
            _connectionString = "jdbc:h2:mem:test";
            _userName = "";
            _password = "";
            _count = Long.MIN_VALUE;

            Class.forName ("org.h2.Driver");
            _initDatabase();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

    public String createExecution(String instanceName, String query) {
        try (Statement statement = DriverManager.getConnection(_connectionString, _userName, _password).createStatement()) {
            String uuid = UUID.randomUUID().toString();

            String insertQuery = "INSERT INTO executions VALUES ( '" + uuid + "', " + _count + ", false, '" + instanceName + "', '" + query + "' );";
            statement.executeUpdate(insertQuery);
            _count += 1;
            return uuid;
        } catch (SQLException e) {
            e.printStackTrace();
            return e.getMessage();
        }
    }

    public String getExecutionStatus(String uuid) {
        try (Statement statement = DriverManager.getConnection(_connectionString, _userName, _password).createStatement()) {
            ResultSet resultSet = statement.executeQuery("SELECT isrunning FROM executions WHERE uuid = '" + uuid + "' ;");

            if (resultSet.next()) {
                boolean isRunning = resultSet.getBoolean("isrunning");
                if (isRunning)
                    return "Running";
                else
                    return "Waiting";
            }

            resultSet.close();
            return null;
        } catch (SQLException e) {
            e.printStackTrace();
            return e.getMessage();
        }
    }

    public void setRunning(String uuid) {
        try (Statement statement = DriverManager.getConnection(_connectionString, _userName, _password).createStatement()) {
            statement.executeUpdate("UPDATE executions SET isrunning = true WHERE uuid = '" + uuid + "' ;");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public Execution getExecution() {
        try (Statement statement = DriverManager.getConnection(_connectionString, _userName, _password).createStatement()) {
            ResultSet result = statement.executeQuery("SELECT * FROM executions WHERE isrunning = false ORDER BY count ASC;");

            if (result.next()) {
                return new Execution(
                        result.getString("uuid"),
                        result.getLong("count"),
                        result.getBoolean("isrunning"),
                        result.getString("instance"),
                        result.getString("query")
                );
            }

            result.close();
            return null;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    public void removeExecution(String uuid) {
        try (Statement statement = DriverManager.getConnection(_connectionString, _userName, _password).createStatement()){
            statement.executeUpdate("DELETE FROM executions WHERE uuid = '" + uuid + "' ;");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
