package cz.cuni.matfyz.collector.model;

import java.util.HashMap;
import java.util.Set;

public class TableData {

    final private String _name;

    public Integer _size; //size of table in bytes
    public Integer _sizeInPages; //size of tables in pages on disk
    public Integer _rowCount; //number of rows
    public Integer _constraintCount;

    private final HashMap<String, ColumnData> _columns;

    public TableData(String name) {
        _columns = new HashMap<>();
        _name = name;
        _size = null;
        _sizeInPages = null;
        _rowCount = null;
        _constraintCount = null;
    }

    //Tables setting methods
    public void setByteSize(int size) {
        if (_size == null)
            _size = size;
    }

    public void setSizeInPages(int sizeInPages) {
        if(_sizeInPages == null)
            _sizeInPages = sizeInPages;
    }

    public void setRowCount(int count) {
        if (_rowCount == null)
            _rowCount = count;
    }

    public void setConstraintCount(int count) {
        if (_constraintCount == null) {
            _constraintCount = count;
        }
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
