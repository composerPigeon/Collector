package cz.cuni.matfyz.collector.model;

import java.util.HashMap;
import java.util.Set;

public class TableData {

    private String _name;

    public int _size; //size of table in bytes
    public int _sizeInPages; //size of tables in pages on disk
    public int _rowCount; //number of rows

    private final HashMap<String, ColumnData> _columns;

    public TableData(String name) {
        _columns = new HashMap<>();
        _name = name;
    }

    //Tables setting methods
    public void setByteSize(int size) {
        _size = size;
    }

    public void setSizeInPages(int sizeInPages) {
        _sizeInPages = sizeInPages;
    }

    public void setRowCount(int count) {
        _rowCount = count;
    }

    public Set<String> getColumnNames() {
        return _columns.keySet();
    }

    //Columns setting methods
    public void setColumnByteSize(String colName, int size) {
        if (_columns.containsKey(colName)) {
            _columns.get(colName).setByteSize(size);
        }
        else {
            _columns.put(colName, new ColumnData(colName));
            _columns.get(colName).setByteSize(size);
        }
    }

    public void setColumnDistinctRatio(String colName, double ratio) {
        if(_columns.containsKey(colName)) {
            _columns.get(colName).setDistinctRatio(ratio);
        }
        else {
            _columns.put(colName, new ColumnData(colName));
            _columns.get(colName).setDistinctRatio(ratio);
        }
    }
}
