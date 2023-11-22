package cz.cuni.matfyz.collector.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DataModel {

    //Gathered data (objects that will be translated to JSON using GSON)
    private String databaseName;

    private String datasetName;
    private double executionTime = 0;

    private QueryData beforeQueryData;
    private QueryData afterQueryData;

    public DataModel(String databaseName, String datasetName) {
        this.databaseName = databaseName;
        this.datasetName = datasetName;

        beforeQueryData = new QueryData();
        afterQueryData = new QueryData();
    }

    public QueryData beforeQuery() {
        return beforeQueryData;
    }

    public QueryData afterQuery() {
        return afterQueryData;
    }

    public void setExecTime(double execTime) { this.executionTime = execTime; }

    public String toJson() throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);

        String dataModelJson = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(this);

        return dataModelJson;
    }
}
