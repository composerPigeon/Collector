package cz.cuni.matfyz.collector.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class QueryDataModel implements DataModel {
    private final QueryData _query;

    public QueryDataModel(String query, String databaseName, String datasetName) {
        _query = new QueryData(query, databaseName, datasetName);
    }

    //ResultData
    @Override
    public void setResultExecutionTime(double time) { _query.getResult().setExecutionTime(time); }

    @Override
    public void setResultByteSize(long size) { _query.getResult().setByteSize(size); }

    @Override
    public void setResultSizeInPages(long size) { _query.getResult().setSizeInPages(size); }

    @Override
    public void setResultRowCount(long count) { _query.getResult().setRowCount(count); }

    //DatasetData
    @Override
    public void setDatasetByteSize(long size) { _query.getDataset().setDataSetSize(size); }
    @Override
    public void setDatasetSizeInPages(long size) { _query.getDataset().setDataSetSizeInPages(size); }
    @Override
    public void setDatasetCacheSize(long size) { _query.getDataset().setDataSetCacheSize(size); }
    @Override
    public void setPageSize(int size) { _query.getDataset().setDataSetPageSize(size); }
    @Override
    public int getPageSize() { return _query.getDataset().getDataSetPageSize(); }

    //TableData
    @Override
    public void setTableByteSize(String tableName, long size) { _query.getDataset().getTable(tableName, true).setByteSize(size); }
    @Override
    public void setTableSizeInPages(String tableName, long size) { _query.getDataset().getTable(tableName, true).setSizeInPages(size); }
    @Override
    public void setTableRowCount(String tableName, long count) { _query.getDataset().getTable(tableName, true).setRowCount(count); }
    @Override
    public void setTableConstraintCount(String tableName, int count) { _query.getDataset().getTable(tableName, true).setConstraintCount(count); }
    @Override
    public void addTable(String tableName) { _query.getDataset().addTable(tableName); }
    @Override
    public Set<String> getTableNames() { return _query.getDataset().getTableNames(); }

    //IndexData
    @Override
    public void setIndexByteSize(String indexName, long size) { _query.getDataset().getIndex(indexName, true).setByteSize(size); }
    @Override
    public void setIndexSizeInPages(String indexName, long size) { _query.getDataset().getIndex(indexName, true).setSizeInPages(size); }
    @Override
    public void setIndexRowCount(String indexName, long count) { _query.getDataset().getIndex(indexName, true).setRowCount(count); }
    @Override
    public void addIndex(String indexName) { _query.getDataset().addIndex(indexName); }
    @Override
    public Set<String> getIndexNames() { return _query.getDataset().getIndexNames(); }

    //ColumnData
    @Override
    public void setColumnMandatory(String tableName, String columnName, boolean mandatory) { _query.getDataset().getTable(tableName, true).getColumn(columnName, true).setMandatory(mandatory); }
    @Override
    public void setColumnDistinctRatio(String tableName, String columnName, double ratio) { _query.getDataset().getTable(tableName, true).getColumn(columnName, true).setDistinctRatio(ratio); }
    @Override
    public int getColumnMaxByteSize(String tableName, String columnName) { return _query.getDataset().getTable(tableName, false).getColumn(columnName, false).getMaxByteSize(); }

    //ColumnTypeData
    @Override
    public void setColumnTypeByteSize(String tableName, String columnName, String typeName, int size) { _query.getDataset().getTable(tableName, true).getColumn(columnName, true).getColumnType(typeName, true).setByteSize(size); }
    @Override
    public void setResultColumnTypeByteSize(String columnName, String typeName, int size) { _query.getResult().getColumn(columnName, true).getColumnType(typeName, true).setByteSize(size);}
    @Override
    public void setColumnTypeRatio(String tableName, String columnName, String typeName, double ratio) { _query.getDataset().getTable(tableName, true).getColumn(columnName, true).getColumnType(typeName, true).setRatio(ratio); }
    @Override
    public void setResultColumnTypeRatio(String columnName, String typeName, double ratio) { _query.getResult().getColumn(columnName, true).getColumnType(typeName, true).setRatio(ratio); }
    @Override
    public void addColumnType(String tableName, String columnName, String typeName) { _query.getDataset().getTable(tableName, true).getColumn(columnName, true).addType(typeName); }
    @Override
    public int getColumnTypeByteSize(String tableName, String columnName, String typeName) { return _query.getDataset().getTable(tableName, false).getColumn(columnName, false).getColumnType(typeName, false).getByteSize(); }

    @Override
    public String toJson() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.writerWithDefaultPrettyPrinter();
        return mapper.writeValueAsString(toMap());
    }

    @Override
    public Map<String, Object> toMap() { return _query.toMap(); }
}

