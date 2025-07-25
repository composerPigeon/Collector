package cz.cuni.matfyz.collector.wrappers.postgresql;

import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractWrapper;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.components.AbstractExplainPlanParser;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.components.AbstractQueryResultParser;
import cz.cuni.matfyz.collector.wrappers.exceptions.*;
import cz.cuni.matfyz.collector.wrappers.postgresql.components.PostgresConnection;
import cz.cuni.matfyz.collector.wrappers.postgresql.components.PostgresDataCollector;
import cz.cuni.matfyz.collector.wrappers.postgresql.components.PostgresExplainPlanParser;
import cz.cuni.matfyz.collector.wrappers.postgresql.components.PostgresQueryResultParser;

import java.sql.*;

/**
 * Class which represents the wrapper operating over PostgresSQL database
 */
public class PostgresWrapper extends AbstractWrapper<ResultSet, String, String> {
    public PostgresWrapper(ConnectionData connectionData) {
        super(connectionData, new PostgresExceptionsFactory(connectionData));
    }


    @Override
    protected AbstractQueryResultParser<ResultSet> createResultParser() {
        return new PostgresQueryResultParser(getExceptionsFactory());
    }

    @Override
    protected AbstractExplainPlanParser<String> createExplainPlanParser() {
        return new PostgresExplainPlanParser(getExceptionsFactory());
    }

    @Override
    protected PostgresConnection createConnection(ExecutionContext<ResultSet, String, String> context) throws ConnectionException {
        return new PostgresConnection(
            PostgresResources.getConnectionLink(
                _connectionData.host(),
                _connectionData.port(),
                _connectionData.databaseName(),
                _connectionData.user(),
                _connectionData.password()
            ),
            getExceptionsFactory()
        );
    }

    @Override
    protected String parseInputQuery(ExecutionContext<ResultSet, String, String> context) {
        return context.getInputQuery();
    }

    @Override
    protected PostgresDataCollector createDataCollector(ExecutionContext<ResultSet, String, String> context) throws DataCollectException {
        try {
            return new PostgresDataCollector(context, _resultParser);
        } catch (ConnectionException e) {
            throw getExceptionsFactory().dataCollectorNotInitialized(e);
        }
    }

    @Override
    public void close() {
    }
}
