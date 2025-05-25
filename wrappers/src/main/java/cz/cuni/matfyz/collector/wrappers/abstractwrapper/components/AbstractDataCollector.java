package cz.cuni.matfyz.collector.wrappers.abstractwrapper.components;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.exceptions.*;
import cz.cuni.matfyz.collector.wrappers.queryresult.CachedResult;
import cz.cuni.matfyz.collector.wrappers.queryresult.ConsumedResult;

/**
 * Class representing entity that is responsible for collecting all statistical data for query
 * @param <TPlan>
 * @param <TResult>
 * @param <TQuery>
 */
public abstract class AbstractDataCollector<TResult, TQuery, TPlan> {
    private final AbstractConnection<TResult, TQuery, TPlan> _connection;
    private final AbstractQueryResultParser<TResult> _resultParser;

    protected final WrapperExceptionsFactory _exceptionsFactory;
    protected final String _databaseName;
    protected final DataModel _model;


    public AbstractDataCollector(
            String databaseName,
            ExecutionContext<TResult, TQuery, TPlan> context,
            AbstractQueryResultParser<TResult> resultParser
    ) throws ConnectionException {
        _databaseName = databaseName;
        _model = context.getModel();
        _connection = context.getConnection();
        _resultParser = resultParser;
        _exceptionsFactory = context.getExceptionsFactory();
    }

    public abstract void collectData(ConsumedResult result) throws DataCollectException;

    protected CachedResult executeQuery(TQuery query) throws DataCollectException {
        try {
            return _resultParser.parseResultAndCache(_connection.executeQuery(query));
        } catch (QueryExecutionException | ParseException e) {
            throw _exceptionsFactory.dataCollectionFailed(e);
        }
    }

    protected ConsumedResult executeQueryAndConsume(TQuery query) throws DataCollectException {
        try {
            return _resultParser.parseResultAndConsume(_connection.executeQuery(query));
        } catch (QueryExecutionException | ParseException e) {
            throw _exceptionsFactory.dataCollectionFailed(e);
        }
    }
}
