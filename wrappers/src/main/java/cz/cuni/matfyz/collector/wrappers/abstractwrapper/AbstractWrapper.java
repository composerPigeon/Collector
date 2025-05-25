package cz.cuni.matfyz.collector.wrappers.abstractwrapper;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.components.*;
import cz.cuni.matfyz.collector.wrappers.exceptions.*;

public abstract class AbstractWrapper<TResult, TQuery, TPlan> implements Wrapper, AutoCloseable {

    protected final ConnectionData _connectionData;

    protected WrapperExceptionsFactory _exceptionsFactory;

    protected AbstractQueryResultParser<TResult> _resultParser;

    protected AbstractExplainPlanParser<TPlan> _explainPlanParser;

    public AbstractWrapper(ConnectionData connectionData) {
        _connectionData = connectionData;
        _exceptionsFactory = createExceptionsFactory();
        _resultParser = createResultParser();
        _explainPlanParser = createExplainPlanParser();
    }

    protected WrapperExceptionsFactory createExceptionsFactory() {
        return new WrapperExceptionsFactory(_connectionData);
    }

    protected abstract AbstractQueryResultParser<TResult> createResultParser();

    protected abstract AbstractExplainPlanParser<TPlan> createExplainPlanParser();

    public final DataModel executeQuery(String query) throws WrapperException {
        var context = createExecutionContext(query, DataModel.CreateForQuery(query, _connectionData.systemName, _connectionData.databaseName));

        try (var connection = createConnection(context)) {
            context.setConnection(connection);
            setDependenciesBeforeExecutionIfNeeded(context);

            var inputQuery = parseInputQuery(query, context);
            var explainResult = connection.executeWithExplain(inputQuery);

            var mainResult = _resultParser.parseResultAndConsume(explainResult.result());
            _explainPlanParser.parsePlan(explainResult.plan(), context.getModel());


            var dataCollector = createDataCollector(context);
            dataCollector.collectData(mainResult);

            removeDependenciesAfterExecutionIfPossible(context);
            return context.getModel();
        }
    }

    protected ExecutionContext<TResult, TQuery, TPlan> createExecutionContext(String query, DataModel model) {
        return new ExecutionContext<>(query, _exceptionsFactory, model);
    }

    protected abstract AbstractConnection<TResult, TQuery, TPlan> createConnection(ExecutionContext<TResult, TQuery, TPlan> context) throws ConnectionException;

    protected void setDependenciesBeforeExecutionIfNeeded(ExecutionContext<TResult, TQuery, TPlan> context) throws WrapperException { }

    protected abstract TQuery parseInputQuery(String query, ExecutionContext<TResult, TQuery, TPlan> context) throws ParseException, WrapperUnsupportedOperationException;

    protected abstract AbstractDataCollector<TResult, TQuery, TPlan> createDataCollector(ExecutionContext<TResult, TQuery, TPlan> context) throws DataCollectException;

    protected void removeDependenciesAfterExecutionIfPossible(ExecutionContext<TResult, TQuery, TPlan> context) throws WrapperException { }



    public record ConnectionData(String host, int port, String systemName, String databaseName, String user, String password) { }
}
