package model.src.main.java;

import java.util.HashMap;

public class TableData {

    private String name;

    public int size; //size of table in bytes
    public int sizeInPages; //size of tables in pages on disk
    public int rowCount; //number of rows

    private HashMap<String, ColumnData> columns;

    public TableData(String name) {
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
            columns.put(colName, new ColumnData(colName)).setByteSize(size);
        }
    }

    public void setColumnDictinctRatio(String colName, double ratio) {
        if(columns.containsKey(colName)) {
            columns.get(colName).setDistinctRatio(ratio);
        }
        else {
            columns.put(colName, new ColumnData(colName)).setDistinctRatio(ratio);
        }
    }

    
    public String toString(int indent) {
        String indentStr = QueryData.createIndentedString(indent);
        StringBuilder str = new StringBuilder();

        str.append(indentStr + "Table name: " + name + "\n");
        str.append(indentStr + "Table size: " + size + "\n");
        str.append(indentStr + "Table size in pages: " + sizeInPages + "\n");
        str.append(indentStr + "Table number of rows: " + rowCount + "\n");
        for (ColumnData data: columns.values()) {
            str.append(data.toString(indent + 1));
        }

        return str.toString();
    }
}
