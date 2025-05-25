package cz.cuni.matfyz.collector.wrappers.postgresql;

import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractWrapper;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.components.AbstractExplainPlanParser;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.components.AbstractQueryResultParser;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.components.ExecutionContext;
import cz.cuni.matfyz.collector.wrappers.exceptions.*;
import cz.cuni.matfyz.collector.wrappers.postgresql.components.PostgresConnection;
import cz.cuni.matfyz.collector.wrappers.postgresql.components.PostgresDataCollector;
import cz.cuni.matfyz.collector.wrappers.postgresql.components.PostgresExplainPlanParser;
import cz.cuni.matfyz.collector.wrappers.postgresql.components.PostgresQueryResultParser;

import java.sql.*;

/**
 * Class which represents the wrapper operating over PostgreSQL database
 */
public class PostgresWrapper extends AbstractWrapper<ResultSet, String, String> {
    public PostgresWrapper(String host, int port, String databaseName, String user, String password) {
        super(new ConnectionData(host, port, PostgresResources.SYSTEM_NAME, databaseName, user, password));
    }

    @Override
    protected WrapperExceptionsFactory createExceptionsFactory() {
        return new PostgresExceptionsFactory(_connectionData);
    }

    @Override
    protected AbstractQueryResultParser<ResultSet> createResultParser() {
        return new PostgresQueryResultParser(_exceptionsFactory);
    }

    @Override
    protected AbstractExplainPlanParser<String> createExplainPlanParser() {
        return new PostgresExplainPlanParser(_exceptionsFactory);
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
            _exceptionsFactory
        );
    }

    @Override
    protected String parseInputQuery(String query, ExecutionContext<ResultSet, String, String> context) {
        return query;
    }

    @Override
    protected PostgresDataCollector createDataCollector(ExecutionContext<ResultSet, String, String> context) throws DataCollectException {
        try {
            return new PostgresDataCollector(context, _resultParser, _connectionData.databaseName());
        } catch (ConnectionException e) {
            throw _exceptionsFactory.dataCollectorNotInitialized(e);
        }
    }

    @Override
    public void close() throws Exception {
    }
}
