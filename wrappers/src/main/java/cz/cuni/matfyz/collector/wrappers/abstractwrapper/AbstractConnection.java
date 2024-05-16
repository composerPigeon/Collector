package cz.cuni.matfyz.collector.wrappers.abstractwrapper;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.cachedresult.ConsumedResult;
import cz.cuni.matfyz.collector.wrappers.exceptions.QueryExecutionException;
import cz.cuni.matfyz.collector.wrappers.cachedresult.CachedResult;

public abstract class AbstractConnection<TPlan, TResult, TQuery> implements AutoCloseable {

    protected AbstractParser<TPlan, TResult> _parser;

    public AbstractConnection(AbstractParser<TPlan, TResult> parser) {
        _parser = parser;
    }

    public abstract CachedResult executeQuery(TQuery query) throws QueryExecutionException;
    public abstract ConsumedResult executeMainQuery(TQuery query, DataModel toModel) throws QueryExecutionException;
}
