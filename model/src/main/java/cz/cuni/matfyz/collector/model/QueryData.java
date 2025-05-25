package cz.cuni.matfyz.collector.model;

import java.util.*;

/**
 * Main class holding gathered statistical data which can be eventually transformed to json for persistent storage
 */
class QueryData implements MapWritable {

    /** Field containing dbType for which is this record relevant */
    private final String _systemName;

    /** Field containing name of dataset */
    private final String _databaseName;

    /** Field containing query for which were these statistical data gathered. */
    private final String _query;

    private final DatabaseData _datasetData;
    private final ResultData _resultData;

    public QueryData(String query, String systemName, String databaseName) {
        _query = query;
        _systemName = systemName;
        _databaseName = databaseName;

        _datasetData = new DatabaseData();
        _resultData = new ResultData();
    }

    public DatabaseData getDatabaseData() {
        return _datasetData;
    }
    public ResultData getResultData() {
        return _resultData;
    }

    public void writeTo(Map<String, Object> map) {
        map.put("query", _query);
        map.put("systemName", _systemName);
        map.put("databaseName", _databaseName);
    }


}
