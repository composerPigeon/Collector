package cz.cuni.matfyz.collector.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;

/**
 * Main class holding gathered statistical data which can be eventually transformed to json for persistent storage
 */
public class DataModel {

    /** Field containing dbType for which is this record relevant */
    private final String _databaseName;

    /** Field containing name of dataset */
    private final String _datasetName;

    /** Field containing query for which were these statistical data gathered. */
    private final String _query;

    private final DatasetData _datasetData;
    private final ResultData _resultData;

    public DataModel(String query, String databaseName, String datasetName) {
        _query = query;
        _databaseName = databaseName;
        _datasetName = datasetName;

        _datasetData = new DatasetData();
        _resultData = new ResultData();
    }

    public DatasetData datasetData() {
        return _datasetData;
    }
    public ResultData resultData() {
        return _resultData;
    }

    @JsonIgnore
    public Set<String> getTableNames() {
        return new HashSet<>(_datasetData.getTableNames());
    }
    @JsonIgnore
    public Set<String> getIndexNames() {
        return new HashSet<>(_datasetData.getIndexNames());
    }
    public int getColumnByteSize(String tableName, String colName) {
        return _datasetData.getColumnByteSize(tableName, colName);
    }
    @JsonIgnore
    public int getPageSize() {
        return _datasetData.getDataSetPageSize();
    }

    /**
     * Method for converting DataModel to json format
     * @return converted json string
     */
    public String toJson() {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
            objectMapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);

            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(this);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }
    }

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
