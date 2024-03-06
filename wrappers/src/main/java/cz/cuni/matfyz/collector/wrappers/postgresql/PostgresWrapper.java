package cz.cuni.matfyz.collector.wrappers.postgresql;

import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractWrapper;
import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.WrapperException;

import java.sql.*;

public class PostgresWrapper extends AbstractWrapper<String, ResultSet> {
    public PostgresWrapper(String link, String datasetName) {
        super(link, datasetName);
    }

    @Override
    public DataModel executeQuery(String query) throws WrapperException {
        try (
           var connection = new PostgresConnection(_link + '/' + _datasetName, "", "");
        ) {
            var parser = new PostgresParser(_datasetName);
            var saver = new PostgresDataSaver(connection, _datasetName);
            connection.executeMainQuery(query);

            DataModel dataModel = parser.parseExplainTree(connection.getExplainTree());
            saver.saveDataTo(dataModel);

            return dataModel;

        } catch (SQLException e) {
            throw new WrapperException(e);
        }
    }

    @Override
    public String toString() {
        return "Connection link: " + _link + "\n";
    }

}
