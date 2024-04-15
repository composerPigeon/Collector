package cz.cuni.matfyz.collector.wrappers.abstractwrapper;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.cachedresult.CachedResult;
import cz.cuni.matfyz.collector.wrappers.exceptions.DataCollectException;

public abstract class AbstractDataCollector<TPlan, TResult> {
    protected String _datasetName;
    protected DataModel _model;
    protected AbstractConnection<TPlan, TResult> _connection;

    public AbstractDataCollector(String datasetName, DataModel model, AbstractConnection<TPlan, TResult> connection) {
        _datasetName = datasetName;
        _model = model;
        _connection = connection;
    }

    public abstract DataModel collectData(CachedResult result) throws DataCollectException;
}
