package cz.cuni.matfyz.collector.wrappers.abstractwrapper;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.exceptions.WrapperException;

/**
 * Class which represents unified API for comunication with all wrappers from server module
 */
public abstract class AbstractWrapper {
    /** Field containing hostName, with which was the connection to the database established */
    protected final String _hostName;
    /** Field containing datasetName, with which was the connection to the database established */
    protected final String _datasetName;
    /** Field containing userName, with which was the connection to the database established */
    protected final String _userName;
    /** Field containing password, with which was the connection to the database established */
    protected final String _password;
    /** Field containing port, with which was the connection to the database established */
    protected final int _port;

    public AbstractWrapper(String host, int port, String datasetName, String user, String password) {
        _hostName = host;
        _datasetName = datasetName;
        _userName = user;
        _password = password;
        _port = port;
    }

    /**
     * Method which is executed by QueryScheduler to compute statistical result of query over this wrapper
     * @param query inputed
     * @return instance of DataModel which contains all measured data
     * @throws WrapperException when some problem occur during process, message of this exception is saved as a result to execution if some error is thrown during evaluation
     */
    public abstract DataModel executeQuery(String query) throws WrapperException;
}
