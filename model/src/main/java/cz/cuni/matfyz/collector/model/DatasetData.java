package cz.cuni.matfyz.collector.model;

import java.util.HashMap;
import java.util.Set;

public class DatasetData {
    private Integer _dataSetSize; //size of dataset in bytes
    private Integer _dataSetSizeInPages; //size of dataset in pages
    private Integer _pageSize;
    
    private final HashMap<String, TableData> _tables;
    private final HashMap<String, IndexData> _indexes;

    public DatasetData() {
        _tables = new HashMap<>();
        _indexes = new HashMap<>();

        _dataSetSize = null;
        _dataSetSizeInPages = null;
        _pageSize = null;
    }

    //Database setting methods
    public void setDataSetSize(int size) {
        if(_dataSetSize == null) { _dataSetSize = size; }
    }

    public void setDataSetSizeInPages(int dataSetSizeInPages) {
        if (_dataSetSizeInPages == null) { _dataSetSizeInPages = dataSetSizeInPages; }
    }
    public void setDataSetPageSize(int pageSize) {
        if (_pageSize == null) { _pageSize = pageSize; }
    }
    public int getDataSetPageSize() {
        return _pageSize;
    }

    //Tables setting methods
    public void setTableByteSize(String tableName, int size) {
        if (_tables.containsKey(tableName)) {
            _tables.get(tableName).setByteSize(size);
        }
        else {
            _tables.put(tableName, new TableData(tableName));
            _tables.get(tableName).setByteSize(size);
        }
    }

    public void setTableSizeInPages(String tableName, int sizeInPages) {
        if (_tables.containsKey(tableName)) {
            _tables.get(tableName).setSizeInPages(sizeInPages);
        }
        else {
            _tables.put(tableName, new TableData(tableName));
            _tables.get(tableName).setSizeInPages(sizeInPages);
        }
    }

    public void setTableRowCount(String tableName, int count) {
        if (_tables.containsKey(tableName)) {
            _tables.get(tableName).setRowCount(count);
        }
        else {
            _tables.put(tableName, new TableData(tableName));
            _tables.get(tableName).setRowCount(count);
        }
    }

    public void setIndexByteSize(String inxName, int size) {
        if (_indexes.containsKey(inxName)) {
            _indexes.get(inxName).setByteSize(size);
        }
        else {
            _indexes.put(inxName, new IndexData(inxName));
            _indexes.get(inxName).setByteSize(size);
        }
    }

    public void setIndexSizeInPages(String inxName, int sizeInPages) {
        if (_indexes.containsKey(inxName)) {
            _indexes.get(inxName).setSizeInPages(sizeInPages);
        }
        else {
            _indexes.put(inxName, new IndexData(inxName));
            _indexes.get(inxName).setSizeInPages(sizeInPages);
        }
    }

    public void setIndexRowCount(String inxName, int count) {
        if (_indexes.containsKey(inxName)) {
            _indexes.get(inxName).setRowCount(count);
        }
        else {
            _indexes.put(inxName, new IndexData(inxName));
            _indexes.get(inxName).setRowCount(count);
        }
    }

    public Set<String> getTableNames() {
        return _tables.keySet();
    }

    public Set<String> getIndexNames() {
        return _indexes.keySet();
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
}
