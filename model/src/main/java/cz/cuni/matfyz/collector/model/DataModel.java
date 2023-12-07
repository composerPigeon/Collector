package cz.cuni.matfyz.collector.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Set;
import java.util.HashSet;

public class DataModel {

    //Gathered data (objects that will be translated to JSON using GSON)
    private String _databaseName;

    private String _datasetName;
    private double _executionTime = 0;

    private QueryData _beforeQueryData;
    private QueryData _afterQueryData;

    public DataModel(String databaseName, String datasetName) {
        _databaseName = databaseName;
        _datasetName = datasetName;

        _beforeQueryData = new QueryData();
        _afterQueryData = new QueryData();
    }

    public QueryData beforeQuery() {
        return _beforeQueryData;
    }
    public QueryData afterQuery() {
        return _afterQueryData;
    }

    public Set<String> getTableNames() {
        Set<String> names = new HashSet<>();
        names.addAll(_beforeQueryData.getTableNames());
        names.addAll(_afterQueryData.getTableNames());
        return names;
    }

    public Set<String> getIndexNames() {
        Set<String> names = new HashSet<>();
        names.addAll(_beforeQueryData.getIndexNames());
        names.addAll(_afterQueryData.getIndexNames());
        return names;
    }

    public void setExecTime(double execTime) { _executionTime = execTime; }

    public String toJson() throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);

        String dataModelJson = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(this);

        return dataModelJson;
    }
}
