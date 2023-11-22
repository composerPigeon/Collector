package cz.cuni.matfyz.collector.model;

import java.util.HashMap;

public class TableData {

    private String name;

    public int size; //size of table in bytes
    public int sizeInPages; //size of tables in pages on disk
    public int rowCount; //number of rows

    private final HashMap<String, ColumnData> columns;

    public TableData(String name) {
        columns = new HashMap<>();
        this.name = name;
    }

    //Tables setting methods
    public void setByteSize(int size) {
        this.size = size;
    }

    public void setSizeInPages(int sizeInPages) {
        this.sizeInPages = sizeInPages;
    }

    public void setRowCount(int count) {
        rowCount = count;
    }

    //Columns setting methods
    public void setColumnByteSize(String colName, int size) {
        if (columns.containsKey(colName)) {
            columns.get(colName).setByteSize(size);
        }
        else {
            columns.put(colName, new ColumnData(colName));
            columns.get(colName).setByteSize(size);
        }
    }

    public void setColumnDistinctRatio(String colName, double ratio) {
        if(columns.containsKey(colName)) {
            columns.get(colName).setDistinctRatio(ratio);
        }
        else {
            columns.put(colName, new ColumnData(colName));
            columns.get(colName).setDistinctRatio(ratio);
        }
    }
}
