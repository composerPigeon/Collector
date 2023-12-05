package cz.cuni.matfyz.collector.model;

import java.util.HashMap;
import java.util.Arrays;

public class QueryData {
    private int _dataSetSize; //size of dataset in bytes
    private int _dataSetSizeInPages; //size of dataset in pages
    
    private final HashMap<String, TableData> _tables;
    private final HashMap<String, IndexData> _indexes;

    public QueryData() {
        _tables = new HashMap<>();
        _indexes = new HashMap<>();
    }

    //Database setting methods
    public void setDataSetSize(int size) {
        _dataSetSize = size;
    }

    public void setDataSetSizeInPages(int dataSetSizeInPages) {
        _dataSetSizeInPages = dataSetSizeInPages;
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

    public String[] getTableNames() {
        String[] result = new String[_tables.size()];
        int i = 0;
        for (String tableName: _tables.keySet()) {
            result[i] = tableName;
            i += 1;
        }
        return result;
    }

    public String[] getIndexNames() {
        String[] result = new String[_indexes.size()];
        int i = 0;
        for (String inxName: _indexes.keySet()) {
            result[i] = inxName;
            i += 1;
        }
        return result;
    }



    //Columns setting methods
    public void setColumnByteSize(String tableName, String colName, int size) {
        if(tables.containsKey(tableName)) {
            tables.get(tableName).setColumnByteSize(colName, size);
        }
        else {
            tables.put(tableName, new TableData(tableName));
            tables.get(tableName).setColumnByteSize(colName, size);
        }
    }

    public void setColumnDistinctRatio(String tableName, String colName, double ratio) {
        if(tables.containsKey(tableName)) {
            tables.get(tableName).setColumnDistinctRatio(colName, ratio);
        }
        else {
            tables.put(tableName, new TableData(tableName));
            tables.get(tableName).setColumnDistinctRatio(colName, ratio);
        }
    }
}
