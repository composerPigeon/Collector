package cz.cuni.matfyz.collector.wrappers.abstractwrapper;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.exceptions.WrapperException;

public abstract class AbstractWrapper {
    protected final String _hostName;
    protected final String _datasetName;
    protected final String _userName;
    protected final String _password;
    protected final int _port;

    public AbstractWrapper(String host, int port, String datasetName, String user, String password) {
        _hostName = host;
        _datasetName = datasetName;
        _userName = user;
        _password = password;
        _port = port;
    }
    
    public abstract DataModel executeQuery(String query) throws WrapperException;
}
