package cz.cuni.matfyz.collector.wrappers.abstractwrapper;

import java.sql.Connection;
import java.sql.SQLException;

import cz.cuni.matfyz.collector.model.DataModel;

public abstract class AbstractWrapper {
    protected String _link;
    protected String _datasetName;
    protected Connection _connection;

    protected abstract DataModel _parseQuery(String query);
    
    public abstract DataModel executeQuery(String query) throws SQLException;
}
