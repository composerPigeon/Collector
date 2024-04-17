package cz.cuni.matfyz.collector.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.HashMap;
import java.util.Set;

public class TableData {

    @JsonIgnore
    final private String _name;
    public Long _size; //size of table in bytes
    public Long _sizeInPages; //size of tables in pages on disk
    public Long _rowCount; //number of rows
    public Long _constraintCount;

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
    public void setByteSize(long size) {
        if (_size == null)
            _size = size;
    }

    public void setSizeInPages(long sizeInPages) {
        if(_sizeInPages == null)
            _sizeInPages = sizeInPages;
    }

    public void setRowCount(long count) {
        if (_rowCount == null)
            _rowCount = count;
    }

    public void setConstraintCount(long count) {
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
            _columns.put(colName, new ColumnData());
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
            _columns.put(colName, new ColumnData());
            _columns.get(colName).setDistinctRatio(ratio);
        }
    }

    public void setColumnType(String colName, String type) {
        if(_columns.containsKey(colName)) {
            _columns.get(colName).setType(type);
        }
        else {
            _columns.put(colName, new ColumnData());
            _columns.get(colName).setType(type);
        }
    }

    public void setColumnMandatory(String colName, boolean value) {
        if(_columns.containsKey(colName)) {
            _columns.get(colName).setMandatory(value);
        }
        else {
            _columns.put(colName, new ColumnData());
            _columns.get(colName).setMandatory(value);
        }
    }
}
