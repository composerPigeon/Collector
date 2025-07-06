package cz.cuni.matfyz.collector.wrappers.abstractwrapper;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.components.*;
import cz.cuni.matfyz.collector.wrappers.exceptions.*;

import java.util.function.Consumer;

public abstract class AbstractWrapper<TResult, TQuery, TPlan> extends AbstractComponent implements Wrapper {

    protected final ConnectionData _connectionData;

    protected AbstractQueryResultParser<TResult> _resultParser;

    protected AbstractExplainPlanParser<TPlan> _explainPlanParser;

    public AbstractWrapper(ConnectionData connectionData, WrapperExceptionsFactory exceptionsFactory) {
        super(exceptionsFactory);
        _connectionData = connectionData;
        _resultParser = createResultParser();
        _explainPlanParser = createExplainPlanParser();
    }

    public AbstractWrapper(ConnectionData connectionData) {
        super(new WrapperExceptionsFactory(connectionData));
        _connectionData = connectionData;
        _resultParser = createResultParser();
        _explainPlanParser = createExplainPlanParser();
        _bindConnectionForContext = null;
    }

    protected abstract AbstractQueryResultParser<TResult> createResultParser();

    protected abstract AbstractExplainPlanParser<TPlan> createExplainPlanParser();

    public final DataModel executeQuery(String query) throws WrapperException {
        var context = createExecutionContext(query, DataModel.CreateForQuery(query, _connectionData.systemName, _connectionData.databaseName));

        try (var connection = createConnection(context)) {
            _bindConnectionForContext.accept(connection);
            setDependenciesBeforeExecutionIfNeeded(context);

            var inputQuery = parseInputQuery(context);
            var explainResult = connection.executeWithExplain(inputQuery);

            var mainResult = _resultParser.parseResultAndConsume(explainResult.result());
            _explainPlanParser.parsePlan(explainResult.plan(), context.getModel());


            var dataCollector = createDataCollector(context);
            dataCollector.collectData(mainResult);

            removeDependenciesAfterExecutionIfPossible(context);
            return context.getModel();
        }
    }

    private Consumer<AbstractConnection<TResult, TQuery, TPlan>> _bindConnectionForContext;

    protected ExecutionContext<TResult, TQuery, TPlan> createExecutionContext(String query, DataModel model) {
        return new ExecutionContext<>(query, this, model);
    }

    protected abstract AbstractConnection<TResult, TQuery, TPlan> createConnection(ExecutionContext<TResult, TQuery, TPlan> context) throws ConnectionException;

    protected void setDependenciesBeforeExecutionIfNeeded(ExecutionContext<TResult, TQuery, TPlan> context) throws WrapperException { }

    protected abstract TQuery parseInputQuery(ExecutionContext<TResult, TQuery, TPlan> context) throws ParseException, WrapperUnsupportedOperationException;

    protected abstract AbstractDataCollector<TResult, TQuery, TPlan> createDataCollector(ExecutionContext<TResult, TQuery, TPlan> context) throws DataCollectException;

    protected void removeDependenciesAfterExecutionIfPossible(ExecutionContext<TResult, TQuery, TPlan> context) throws WrapperException { }



    public record ConnectionData(String host, int port, String systemName, String databaseName, String user, String password) { }

    public static class ExecutionContext<TResult, TQuery, TPlan> extends AbstractComponent {
        private final DataModel _model;
        private final String _inputQuery;
        private final ConnectionData _connectionData;

        public ExecutionContext(
                String query,
                AbstractWrapper<TResult, TQuery, TPlan> wrapper,
                DataModel model
        ) {
            super(wrapper.getExceptionsFactory());
            wrapper._bindConnectionForContext = this::_setConnection;
            _connectionData = wrapper._connectionData;
            _inputQuery = query;
            _model = model;
        }

        private AbstractConnection<TResult, TQuery, TPlan> _connection = null;


        private void _setConnection(AbstractConnection<TResult, TQuery, TPlan> connection) {
            if (_connection == null)
                _connection = connection;
        }

        public <T> T getConnection(Class<T> clazz) throws ConnectionException {
            if (_connection == null)
                throw getExceptionsFactory().connectionIsNull();
            else if (_connection.isOpen())
                return clazz.cast(_connection);
            else
                throw getExceptionsFactory().connectionNotOpen();
        }

        public AbstractConnection<TResult, TQuery, TPlan> getConnection() throws ConnectionException {
            if (_connection == null)
                throw getExceptionsFactory().connectionIsNull();
            else if (_connection.isOpen())
                return _connection;
            else
                throw getExceptionsFactory().connectionNotOpen();
        }

        public String getInputQuery() {
            return _inputQuery;
        }

        public DataModel getModel() {
            return _model;
        }

        public String getDatabaseName() {
            return _connectionData.databaseName;
        }

        public String getSystemName() {
            return _connectionData.systemName;
        }
    }
}
