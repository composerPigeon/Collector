package cz.cuni.matfyz.collector.wrappers.abstractwrapper;

import cz.cuni.matfyz.collector.model.DataModel;

public abstract class AbstractWrapper<P, R> {
    protected String _link;
    protected String _datasetName;

    public AbstractWrapper(String link, String datasetName) {
        _link = link;
        _datasetName = datasetName;
    }
    
    public abstract DataModel executeQuery(String query) throws WrapperException;
}
