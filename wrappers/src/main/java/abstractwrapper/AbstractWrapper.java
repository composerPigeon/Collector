package wrappers.src.main.java.abstractwrapper;

import java.sql.Connection;
import java.sql.SQLException;

import model.src.main.java.DataModel;

public abstract class AbstractWrapper {
    protected String link;
    protected Connection connection;

    protected abstract DataModel parseQuery(String query);
    
    public abstract DataModel executeQuery(String query) throws SQLException;
}
