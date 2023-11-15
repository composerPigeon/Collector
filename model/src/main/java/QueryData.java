package model.src.main.java;

import java.util.HashMap;
import java.util.Arrays;

public class QueryData {
    private int size; //size of database in bytes
    private int sizeInPages; //size of database in pages
    
    private HashMap<String, TableData> tables;
    private HashMap<String, TableData> indexes;

    //Database setting methods
    public void setByteSize(int size) {
        this.size = size;
    }

    public void setSizeInPages(int sizeInPages) {
        this.sizeInPages = sizeInPages;
    }

    //Tables setting methods
    public void setTableByteSize(String tableName, int size) {
        if (tables.containsKey(tableName)) {
            tables.get(tableName).setByteSize(size);
        }
        else {
            tables.put(tableName, new TableData(tableName)).setByteSize(size);
        }
    }

    public void setTableSizeInPages(String tableName, int sizeInPages) {
        if (tables.containsKey(tableName)) {
            tables.get(tableName).setSizeInPages(sizeInPages);
        }
        else {
            tables.put(tableName, new TableData(tableName)).setSizeInPages(sizeInPages);
        }
    }

    public void setTableRowCount(String tableName, int count) {
        if (tables.containsKey(tableName)) {
            tables.get(tableName).setRowCount(count);
        }
        else {
            tables.put(tableName, new TableData(tableName)).setRowCount(count);
        }
    }

    //Columns setting methods
    public void setColumnByteSize(String tableName, String colName, int size) {
        if(tables.containsKey(tableName)) {
            tables.get(tableName).setColumnByteSize(colName, size);
        }
        else {
            tables.put(tableName, new TableData(tableName)).setColumnByteSize(colName, size);
        }
    }

    public void setColumnDictinctRatio(String tableName, String colName, double ratio) {
        if(tables.containsKey(tableName)) {
            tables.get(tableName).setColumnDictinctRatio(colName, ratio);
        }
        else {
            tables.put(tableName, new TableData(tableName)).setColumnDictinctRatio(colName, ratio);
        }
    }


    //toString methods
    public static String createIndentedString(int indent) {
        if (indent > 0) {
            char[] str = new char[indent];
            Arrays.fill(str, '\t');
            return new String(str);
        }
        else if (indent == 0) {
            return "";
        }
        else {
            //TODO: throw argument exception
            return null;
        }
    }

    public String toString(int indent) {
        String indentStr = QueryData.createIndentedString(indent);
        StringBuilder str = new StringBuilder();

        str.append(indentStr + "Database size: " + size + "\n");
        str.append(indentStr + "Database page size: " + sizeInPages + "\n");
        str.append(indentStr + "Number of used tables: " + tables.size() + "\n");
        str.append(indentStr + "Used tables:\n");
        for(TableData data: tables.values()) {
            str.append(data.toString(indent + 1));
        }
        str.append(indentStr + "Number of used indexes: " + indexes.size() + "\n");
        str.append(indentStr + "Used indexes:\n");
        for (TableData data: indexes.values()) {
            str.append(data.toString(indent + 1));
        }

        return str.toString();
    }
}
