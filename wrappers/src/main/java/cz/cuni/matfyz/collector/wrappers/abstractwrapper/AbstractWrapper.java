package cz.cuni.matfyz.collector.wrappers.abstractwrapper;

import java.sql.Connection;
import java.sql.SQLException;

import cz.cuni.matfyz.collector.model.DataModel;

public abstract class AbstractWrapper {
    protected String link;
    protected String datasetName;
    protected Connection connection;

    protected abstract DataModel parseQuery(String query);
    
    public abstract DataModel executeQuery(String query) throws SQLException;
}
