package cz.cuni.matfyz.collector.wrappers.abstractwrapper;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.exceptions.WrapperException;

public abstract class AbstractWrapper<TPlan, TResult> {
    protected final String _hostName;
    protected final String _datasetName;
    protected final String _userName;
    protected final String _password;

    public AbstractWrapper(String host, String datasetName, String user, String password) {
        _hostName = host;
        _datasetName = datasetName;
        _userName = user;
        _password = password;
    }
    
    public abstract DataModel executeQuery(String query) throws WrapperException;
}
