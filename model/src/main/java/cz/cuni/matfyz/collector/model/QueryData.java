package cz.cuni.matfyz.collector.model;

import java.util.*;

/**
 * Main class holding gathered statistical data which can be eventually transformed to json for persistent storage
 */
public class QueryData {

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

    public Set<String> getTableNames() {
        return new HashSet<>(_datasetData.getTableNames());
    }
    public Set<String> getIndexNames() {
        return new HashSet<>(_datasetData.getIndexNames());
    }

    /*
     * Method for converting DataModel to json format
     * @return converted json string
     */
    /*
    public String toJson() throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);

        return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(this);
    }
     */

    /**
     * Method for converting DataModel to Map
     * @return converted Map
     */
    public Map<String, Object> toMap() {
        Map<String, Object> result = new LinkedHashMap<>();

        result.put("query", _query);
        result.put("databaseName", _databaseName);
        result.put("datasetName", _datasetName);
        result.put("datasetData", _datasetData.toMap());
        result.put("resultData", _resultData.toMap());
        return result;
    }


}
