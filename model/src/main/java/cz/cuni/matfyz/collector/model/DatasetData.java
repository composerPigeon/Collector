package cz.cuni.matfyz.collector.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.HashMap;
import java.util.Set;

public class DatasetData {
    private Long _dataSetSize; //size of dataset in bytes
    private Long _dataSetSizeInPages; //size of dataset in pages
    private Integer _pageSize;
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

    //Database setting methods
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
    public void setColumnByteSize(String tableName, String colName, int size) {
        if(_tables.containsKey(tableName)) {
            _tables.get(tableName).setColumnByteSize(colName, size);
        }
        else {
            _tables.put(tableName, new TableData(tableName));
            _tables.get(tableName).setColumnByteSize(colName, size);
        }
    }

    public int getColumnByteSize(String tableName, String colName) {
        if (_tables.containsKey(tableName)) {
            return _tables.get(tableName).getColumnByteSize(colName);
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

    public void setColumnType(String tableName, String colName, String type) {
        if(_tables.containsKey(tableName)) {
            _tables.get(tableName).setColumnType(colName, type);
        }
        else {
            _tables.put(tableName, new TableData(tableName));
            _tables.get(tableName).setColumnType(colName, type);
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
}
