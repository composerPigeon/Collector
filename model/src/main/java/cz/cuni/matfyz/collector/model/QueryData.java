package cz.cuni.matfyz.collector.model;

import java.util.*;

/**
 * Main class holding gathered statistical data which can be eventually transformed to json for persistent storage
 */
public class QueryData implements MapWritable {

    /** Field containing dbType for which is this record relevant */
    private final String _databaseName;

    /** Field containing name of dataset */
    private final String _datasetName;

    /** Field containing query for which were these statistical data gathered. */
    private final String _query;

    private final DatasetData _datasetData;
    private final ResultData _resultData;

    public QueryData(String query, String databaseName, String datasetName) {
        _query = query;
        _databaseName = databaseName;
        _datasetName = datasetName;

        _datasetData = new DatasetData();
        _resultData = new ResultData();
    }

    public DatasetData getDataset() {
        return _datasetData;
    }
    public ResultData getResult() {
        return _resultData;
    }

    public void writeTo(Map<String, Object> map) {
        map.put("query", _query);
        map.put("databaseName", _databaseName);
        map.put("datasetName", _datasetName);
    }


}
