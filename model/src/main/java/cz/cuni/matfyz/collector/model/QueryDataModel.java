package cz.cuni.matfyz.collector.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

class QueryDataModel implements DataModel {
    private final QueryData _query;

    public QueryDataModel(String query, String databaseName, String datasetName) {
        _query = new QueryData(query, databaseName, datasetName);
    }

    //ResultData
    @Override
    public void setResultExecutionTime(double time) { _query.getResultData().setExecutionTime(time); }

    @Override
    public void setResultByteSize(long size) { _query.getResultData().setByteSize(size); }

    @Override
    public void setResultSizeInPages(long size) { _query.getResultData().setSizeInPages(size); }

    @Override
    public void setResultRowCount(long count) { _query.getResultData().setRowCount(count); }

    //DatasetData
    @Override
    public void setDatabaseByteSize(long size) { _query.getDatabaseData().setDatabaseSize(size); }
    @Override
    public void setDatabaseSizeInPages(long size) { _query.getDatabaseData().setDatabaseSizeInPages(size); }
    @Override
    public void setDatabaseCacheSize(long size) { _query.getDatabaseData().setDatabaseCacheSize(size); }
    @Override
    public void setPageSize(int size) { _query.getDatabaseData().setDatabasePageSize(size); }
    @Override
    public int getPageSize() { return _query.getDatabaseData().getDatabasePageSize(); }

    //TableData
    @Override
    public void setTableByteSize(String tableName, long size) { _query.getDatabaseData().getTable(tableName, true).setByteSize(size); }
    @Override
    public void setTableSizeInPages(String tableName, long size) { _query.getDatabaseData().getTable(tableName, true).setSizeInPages(size); }
    @Override
    public void setTableRowCount(String tableName, long count) { _query.getDatabaseData().getTable(tableName, true).setRowCount(count); }
    @Override
    public void setTableConstraintCount(String tableName, int count) { _query.getDatabaseData().getTable(tableName, true).setConstraintCount(count); }
    @Override
    public void addTable(String tableName) { _query.getDatabaseData().addTable(tableName); }
    @Override
    public Set<String> getTableNames() { return _query.getDatabaseData().getTableNames(); }

    //IndexData
    @Override
    public void setIndexByteSize(String indexName, long size) { _query.getDatabaseData().getIndex(indexName, true).setByteSize(size); }
    @Override
    public void setIndexSizeInPages(String indexName, long size) { _query.getDatabaseData().getIndex(indexName, true).setSizeInPages(size); }
    @Override
    public void setIndexRowCount(String indexName, long count) { _query.getDatabaseData().getIndex(indexName, true).setRowCount(count); }
    @Override
    public void addIndex(String indexName) { _query.getDatabaseData().addIndex(indexName); }
    @Override
    public Set<String> getIndexNames() { return _query.getDatabaseData().getIndexNames(); }

    //ColumnData
    @Override
    public void setColumnMandatory(String tableName, String columnName, boolean mandatory) { _query.getDatabaseData().getTable(tableName, true).getColumn(columnName, true).setMandatory(mandatory); }
    @Override
    public void setColumnDistinctRatio(String tableName, String columnName, double ratio) { _query.getDatabaseData().getTable(tableName, true).getColumn(columnName, true).setDistinctRatio(ratio); }
    @Override
    public int getColumnMaxByteSize(String tableName, String columnName) { return _query.getDatabaseData().getTable(tableName, false).getColumn(columnName, false).getMaxByteSize(); }

    //ColumnTypeData
    @Override
    public void setColumnTypeByteSize(String tableName, String columnName, String typeName, int size) { _query.getDatabaseData().getTable(tableName, true).getColumn(columnName, true).getColumnType(typeName, true).setByteSize(size); }
    @Override
    public void setResultColumnTypeByteSize(String columnName, String typeName, int size) { _query.getResultData().getColumn(columnName, true).getColumnType(typeName, true).setByteSize(size);}
    @Override
    public void setColumnTypeRatio(String tableName, String columnName, String typeName, double ratio) { _query.getDatabaseData().getTable(tableName, true).getColumn(columnName, true).getColumnType(typeName, true).setRatio(ratio); }
    @Override
    public void setResultColumnTypeRatio(String columnName, String typeName, double ratio) { _query.getResultData().getColumn(columnName, true).getColumnType(typeName, true).setRatio(ratio); }
    @Override
    public void addColumnType(String tableName, String columnName, String typeName) { _query.getDatabaseData().getTable(tableName, true).getColumn(columnName, true).addType(typeName); }
    @Override
    public int getColumnTypeByteSize(String tableName, String columnName, String typeName) { return _query.getDatabaseData().getTable(tableName, false).getColumn(columnName, false).getColumnType(typeName, false).getByteSize(); }

    @Override
    public String toJson() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.writerWithDefaultPrettyPrinter();
        return mapper.writeValueAsString(toMap());
    }

    private Map<String, Object> writeToMap(String key, MapWritable value, Map<String, Object> rootMap) {
        var map = new LinkedHashMap<String, Object>();
        value.writeTo(map);
        if (rootMap != null && key != null)
            rootMap.put(key, map);
        return map;
    }

    private void writeItemsTo(Map<String, Object> map, MapWritableCollection<? extends MapWritable> collection) {
        var itemsMap = new LinkedHashMap<String, Object>();
        for (var entry : collection.getItems()) {
            var itemMap = writeToMap(entry.getKey(), entry.getValue(), itemsMap);
            var next = collection.getCollectionFor(entry.getKey());
            if (next != null)
                writeItemsTo(itemMap, next);
        }
        collection.AppendTo(map, itemsMap);
    }

    @Override
    public Map<String, Object> toMap() {
        var queryMap = writeToMap(null, _query, null);

        var datasetMap = writeToMap("databaseData", _query.getDatabaseData(), queryMap);

        writeItemsTo(datasetMap, _query.getDatabaseData().tables);

        writeItemsTo(datasetMap, _query.getDatabaseData().indexes);

        var resultMap = writeToMap("resultData", _query.getResultData(), queryMap);
        var resultTableMap = writeToMap("resultTable", _query.getResultData().getResultTable(), resultMap);
        writeItemsTo(resultTableMap, _query.getResultData().getResultTable());

        return queryMap;
    }
}

