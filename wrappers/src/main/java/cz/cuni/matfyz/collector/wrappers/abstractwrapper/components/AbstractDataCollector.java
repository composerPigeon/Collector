package cz.cuni.matfyz.collector.wrappers.abstractwrapper.components;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractWrapper;
import cz.cuni.matfyz.collector.wrappers.exceptions.*;
import cz.cuni.matfyz.collector.wrappers.queryresult.CachedResult;
import cz.cuni.matfyz.collector.wrappers.queryresult.ConsumedResult;

/**
 * Class representing entity that is responsible for collecting all statistical data for query
 * @param <TPlan>
 * @param <TResult>
 * @param <TQuery>
 */
public abstract class AbstractDataCollector<TResult, TQuery, TPlan> extends AbstractComponent {
    private final AbstractQueryResultParser<TResult> _resultParser;
    private final AbstractWrapper.ExecutionContext<TResult, TQuery, TPlan> _context;


    public AbstractDataCollector(
            AbstractWrapper.ExecutionContext<TResult, TQuery, TPlan> context,
            AbstractQueryResultParser<TResult> resultParser
    ) {        super(context.getExceptionsFactory());
        _context = context;
        _resultParser = resultParser;
    }

    public abstract void collectData(ConsumedResult result) throws DataCollectException;

    private AbstractConnection<TResult, TQuery, TPlan> _getConnection() throws DataCollectException {
        try {
            return _context.getConnection();
        } catch (ConnectionException e) {
            throw getExceptionsFactory().dataCollectionFailed(e);
        }
    }

    protected CachedResult executeQuery(TQuery query) throws DataCollectException {
        try {
            return _resultParser.parseResultAndCache(_getConnection().executeQuery(query));
        } catch (QueryExecutionException | ParseException e) {
            throw getExceptionsFactory().dataCollectionFailed(e);
        }
    }

    protected ConsumedResult executeQueryAndConsume(TQuery query) throws DataCollectException {
        try {
            return _resultParser.parseResultAndConsume(_getConnection().executeQuery(query));
        } catch (QueryExecutionException | ParseException e) {
            throw getExceptionsFactory().dataCollectionFailed(e);
        }
    }

    protected DataModel getModel() {
        return _context.getModel();
    }

    protected String getDatabaseName() {
        return _context.getDatabaseName();
    }
}
