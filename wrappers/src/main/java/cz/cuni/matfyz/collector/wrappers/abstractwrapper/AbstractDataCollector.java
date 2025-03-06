package cz.cuni.matfyz.collector.wrappers.abstractwrapper;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.queryresult.ConsumedResult;
import cz.cuni.matfyz.collector.wrappers.exceptions.DataCollectException;

/**
 * Class representing entity that is responsible for collecting all statistical data for query
 * @param <TPlan>
 * @param <TResult>
 * @param <TQuery>
 */
public abstract class AbstractDataCollector<TPlan, TResult, TQuery> {
    protected String _datasetName;
    protected DataModel _model;
    protected AbstractConnection<TPlan, TResult, TQuery> _connection;

    public AbstractDataCollector(String datasetName, DataModel model, AbstractConnection<TPlan, TResult, TQuery> connection) {
        _datasetName = datasetName;
        _model = model;
        _connection = connection;
    }

    public abstract DataModel collectData(ConsumedResult result) throws DataCollectException;
}
