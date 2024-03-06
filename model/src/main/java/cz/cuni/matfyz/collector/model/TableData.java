package cz.cuni.matfyz.collector.model;

import java.util.HashMap;
import java.util.Set;

public class TableData {

    final private String _name;

    public int _size; //size of table in bytes
    public int _sizeInPages; //size of tables in pages on disk
    public int _rowCount; //number of rows

    private final HashMap<String, ColumnData> _columns;

    public TableData(String name) {
        _columns = new HashMap<>();
        _name = name;
        _size = -1;
        _sizeInPages = -1;
        _rowCount = -1;
    }

    //Tables setting methods
    public void setByteSize(int size) {
        if (_size == -1) {_size = size;}
    }

    public void setSizeInPages(int sizeInPages) {
        if(_sizeInPages == -1) {_sizeInPages = sizeInPages;}
    }

    public void setRowCount(int count) {
        if (_rowCount == -1) {_rowCount = count;}
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

    public int getColumnByteSize(String colName) {
        if (_columns.containsKey(colName)) {
            return _columns.get(colName).getByteSize();
        }
        throw new IllegalArgumentException("Column " + colName + " in table " + _name + " does not exists");
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
