package cz.cuni.matfyz.collector.wrappers.abstractwrapper;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.queryresult.ConsumedResult;
import cz.cuni.matfyz.collector.wrappers.exceptions.QueryExecutionException;
import cz.cuni.matfyz.collector.wrappers.queryresult.CachedResult;

/**
 * Class which encapsulates connection resources to some database and provides unified API for communication with this database
 * @param <TPlan> type of execution plan which will be parsed by parser
 * @param <TResult> type of result which will be parsed by parser
 * @param <TQuery> type of query on which all execute methods will be called
 */
public abstract class AbstractConnection<TPlan, TResult, TQuery> implements AutoCloseable {

    protected AbstractParser<TPlan, TResult> _parser;

    public AbstractConnection(AbstractParser<TPlan, TResult> parser) {
        _parser = parser;
    }

    /**
     * Method for executing query from execution. Whole result from database is then cached into memory
     * @param query inputed query
     * @return result of this query
     * @throws QueryExecutionException when some problem occur during the process
     */
    public abstract CachedResult executeQuery(TQuery query) throws QueryExecutionException;

    /**
     * Method for executing query and computing statistical data about result, which are then returned as Consumed result. The result is computed without caching.
     * @param query inputed query
     * @return consumed result of this query, which holds statistical data of result such as rowCount or byteSize...
     * @throws QueryExecutionException when some problem occur during the process
     */
    public abstract ConsumedResult executeQueryAndConsume(TQuery query) throws QueryExecutionException;

    /**
     * Mathod for executing main query, which is consumed, but also includes some explain tree parsing etc.
     * @param query inputed query
     * @param toModel DataModel which is used for storing data parsed from explain tree
     * @return consumed result of this query
     * @throws QueryExecutionException when some problem occur during process
     */
    public abstract ConsumedResult executeMainQuery(TQuery query, DataModel toModel) throws QueryExecutionException;
}
