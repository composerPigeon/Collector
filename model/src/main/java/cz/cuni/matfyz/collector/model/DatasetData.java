package cz.cuni.matfyz.collector.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.*;

/** Class holding statistical data about dataset */
public class DatasetData implements Mappable<String, Object> {

    /** Field containing size of dataset in bytes */
    private Long _dataSetSize;
    /** Field containing size of dataset in pages (virtual disk block size) */
    private Long _dataSetSizeInPages;
    /** Field containing size of page in bytes */
    private Integer _pageSize;
    /** Field containing size of caches in bytes which could be used for query caching */
    private Long _cacheSize;

    private final HashMap<String, TableData> _tables;
    private final HashMap<String, IndexData> _indexes;

    public DatasetData() {
        _tables = new HashMap<>();
        _indexes = new HashMap<>();

        _dataSetSize = null;
        _dataSetSizeInPages = null;
        _pageSize = null;
        _cacheSize = null;
    }

    // Database setting methods
    public void setDataSetSize(long size) {
        if(_dataSetSize == null)
            _dataSetSize = size;
    }

    public void setDataSetSizeInPages(long dataSetSizeInPages) {
        if (_dataSetSizeInPages == null) { _dataSetSizeInPages = dataSetSizeInPages; }
    }
    public void setDataSetPageSize(int pageSize) {
        if (_pageSize == null)
            _pageSize = pageSize;
    }
    @JsonIgnore
    public int getDataSetPageSize() {
        return _pageSize;
    }

    public void setDataSetCacheSize(long size) {
        if(_cacheSize == null)
            _cacheSize = size;
    }

    //Tables setting methods
    public void setTableByteSize(String tableName, long size) {
        if (_tables.containsKey(tableName)) {
            _tables.get(tableName).setByteSize(size);
        }
        else {
            _tables.put(tableName, new TableData(tableName));
            _tables.get(tableName).setByteSize(size);
        }
    }

    public void setTableSizeInPages(String tableName, long sizeInPages) {
        if (_tables.containsKey(tableName)) {
            _tables.get(tableName).setSizeInPages(sizeInPages);
        }
        else {
            _tables.put(tableName, new TableData(tableName));
            _tables.get(tableName).setSizeInPages(sizeInPages);
        }
    }

    public void setTableRowCount(String tableName, long count) {
        if (_tables.containsKey(tableName)) {
            _tables.get(tableName).setRowCount(count);
        }
        else {
            _tables.put(tableName, new TableData(tableName));
            _tables.get(tableName).setRowCount(count);
        }
    }

    public void setTableConstraintCount(String tableName, long count) {
        if (_tables.containsKey(tableName)) {
            _tables.get(tableName).setConstraintCount(count);
        } else {
            _tables.put(tableName, new TableData(tableName));
            _tables.get(tableName).setConstraintCount(count);
        }
    }

    // Indexes setting methods
    public void setIndexByteSize(String inxName, long size) {
        if (_indexes.containsKey(inxName)) {
            _indexes.get(inxName).setByteSize(size);
        }
        else {
            _indexes.put(inxName, new IndexData());
            _indexes.get(inxName).setByteSize(size);
        }
    }

    public void setIndexSizeInPages(String inxName, long sizeInPages) {
        if (_indexes.containsKey(inxName)) {
            _indexes.get(inxName).setSizeInPages(sizeInPages);
        }
        else {
            _indexes.put(inxName, new IndexData());
            _indexes.get(inxName).setSizeInPages(sizeInPages);
        }
    }

    public void setIndexRowCount(String inxName, long count) {
        if (_indexes.containsKey(inxName)) {
            _indexes.get(inxName).setRowCount(count);
        }
        else {
            _indexes.put(inxName, new IndexData());
            _indexes.get(inxName).setRowCount(count);
        }
    }


    @JsonIgnore
    public Set<String> getTableNames() {
        return _tables.keySet();
    }
    public void addTable(String tableName) {
        if (!_tables.containsKey(tableName)) {
            _tables.put(tableName, new TableData(tableName));
        }
    }

    @JsonIgnore
    public Set<String> getIndexNames() {
        return _indexes.keySet();
    }
    public void addIndex(String inxName) {
        if(!_indexes.containsKey(inxName)) {
            _indexes.put(inxName, new IndexData());
        }
    }

    //Columns setting methods
    public void setColumnTypeByteSize(String tableName, String colName, String colType, int size) {
        if(_tables.containsKey(tableName)) {
            _tables.get(tableName).setColumnTypeByteSize(colName, colType, size);
        }
        else {
            _tables.put(tableName, new TableData(tableName));
            _tables.get(tableName).setColumnTypeByteSize(colName, colType, size);
        }
    }

    public int getColumnMaxByteSize(String tableName, String colName) {
        if (_tables.containsKey(tableName)) {
            return _tables.get(tableName).getColumnMaxByteSize(colName);
        }
        throw new IllegalArgumentException("TableName " + tableName + " does not exists in DataModel");
    }

    public void setColumnDistinctRatio(String tableName, String colName, double ratio) {
        if(_tables.containsKey(tableName)) {
            _tables.get(tableName).setColumnDistinctRatio(colName, ratio);
        }
        else {
            _tables.put(tableName, new TableData(tableName));
            _tables.get(tableName).setColumnDistinctRatio(colName, ratio);
        }
    }

    public void addColumnType(String tableName, String colName, String type) {
        if(_tables.containsKey(tableName)) {
            _tables.get(tableName).addColumnType(colName, type);
        }
        else {
            _tables.put(tableName, new TableData(tableName));
            _tables.get(tableName).addColumnType(colName, type);
        }
    }

    public void setColumnMandatory(String tableName, String colName, boolean value) {
        if(_tables.containsKey(tableName)) {
            _tables.get(tableName).setColumnMandatory(colName, value);
        }
        else {
            _tables.put(tableName, new TableData(tableName));
            _tables.get(tableName).setColumnMandatory(colName, value);
        }
    }

    /**
     * private method for converting _tables map to valid map that can be in future converted to json
     * @return converted map
     */
    private Map<String, Object> _parseTablesToMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        for (var entry : _tables.entrySet()) {
            map.put(entry.getKey(), entry.getValue().toMap());
        }
        return map;
    }

    /**
     * private method for converting _indexes map to valid map that can be in future converted to json
     * @return converted map
     */
    private Map<String, Object> _parseIndexesToMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        for (var entry : _indexes.entrySet()) {
            map.put(entry.getKey(), entry.getValue().toMap());
        }
        return map;
    }

    /**
     * Method converting DatasetData to map, that can be stored in org.bson.Document
     * @return converted map
     */
    public Map<String, Object> toMap() {
        Map<String, Object> result = new LinkedHashMap<>();
        if (_dataSetSize != null)
            result.put("datasetSize", _dataSetSize);
        if (_dataSetSizeInPages != null)
            result.put("datasetSizeInPages", _dataSetSizeInPages);
        if (_pageSize != null)
            result.put("pageSize", _pageSize);
        if (_cacheSize != null)
            result.put("cacheSize", _cacheSize);

        result.put("tables", _parseTablesToMap());
        result.put("indexes", _parseIndexesToMap());

        return result;
    }
}
