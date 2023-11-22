package cz.cuni.matfyz.collector.model;

import java.util.HashMap;
import java.util.Arrays;

public class QueryData {
    private int dataSetSize; //size of dataset in bytes
    private int dataSetSizeInPages; //size of dataset in pages
    
    private final HashMap<String, TableData> tables;
    private final HashMap<String, IndexData> indexes;

    public QueryData() {
        tables = new HashMap<>();
        indexes = new HashMap<>();
    }

    //Database setting methods
    public void setDataSetSize(int size) {
        this.dataSetSize = size;
    }

    public void setDataSetSizeInPages(int dataSetSizeInPages) {
        this.dataSetSizeInPages = dataSetSizeInPages;
    }

    //Tables setting methods
    public void setTableByteSize(String tableName, int size) {
        if (tables.containsKey(tableName)) {
            tables.get(tableName).setByteSize(size);
        }
        else {
            tables.put(tableName, new TableData(tableName));
            tables.get(tableName).setByteSize(size);
        }
    }

    public void setTableSizeInPages(String tableName, int sizeInPages) {
        if (tables.containsKey(tableName)) {
            tables.get(tableName).setSizeInPages(sizeInPages);
        }
        else {
            tables.put(tableName, new TableData(tableName));
            tables.get(tableName).setSizeInPages(sizeInPages);
        }
    }

    public void setTableRowCount(String tableName, int count) {
        if (tables.containsKey(tableName)) {
            tables.get(tableName).setRowCount(count);
        }
        else {
            tables.put(tableName, new TableData(tableName));
            tables.get(tableName).setRowCount(count);
        }
    }

    public void setIndexByteSize(String inxName, int size) {
        if (indexes.containsKey(inxName)) {
            indexes.get(inxName).setByteSize(size);
        }
        else {
            indexes.put(inxName, new IndexData(inxName));
            indexes.get(inxName).setByteSize(size);
        }
    }

    public void setIndexSizeInPages(String inxName, int sizeInPages) {
        if (indexes.containsKey(inxName)) {
            indexes.get(inxName).setSizeInPages(sizeInPages);
        }
        else {
            indexes.put(inxName, new IndexData(inxName));
            indexes.get(inxName).setSizeInPages(sizeInPages);
        }
    }

    public void setIndexRowCount(String inxName, int count) {
        if (indexes.containsKey(inxName)) {
            indexes.get(inxName).setRowCount(count);
        }
        else {
            indexes.put(inxName, new IndexData(inxName));
            indexes.get(inxName).setRowCount(count);
        }
    }

    public String[] getTableNames() {
        String[] result = new String[tables.size()];
        int i = 0;
        for (String tableName: tables.keySet()) {
            result[i] = tableName;
            i += 1;
        }
        return result;
    }

    public String[] getIndexNames() {
        String[] result = new String[tables.size()];
        int i = 0;
        for (String inxName: indexes.keySet()) {
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
