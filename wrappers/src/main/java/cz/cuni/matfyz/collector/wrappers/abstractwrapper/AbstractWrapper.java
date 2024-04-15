package cz.cuni.matfyz.collector.wrappers.abstractwrapper;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.exceptions.WrapperException;

public abstract class AbstractWrapper<TPlan, TResult> {
    protected String _link;
    protected String _datasetName;
    public AbstractWrapper(String link, String datasetName) {
        _link = link;
        _datasetName = datasetName;
    }
    
    public abstract DataModel executeQuery(String query) throws WrapperException;
}
