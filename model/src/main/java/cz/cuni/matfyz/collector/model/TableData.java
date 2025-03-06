package cz.cuni.matfyz.collector.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Class holding statistical data about table
 */
public class TableData implements Mappable<String, Object> {

    /** Field containing table name */
    @JsonIgnore
    final private String _name;
    /** Field containing size of table in bytes */
    public Long _size;
    /** Field containing size of table in pages */
    public Long _sizeInPages;
    /** Field containing row count of table */
    public Long _rowCount;
    /** Field containing number of constraints defined over table */
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
    public void setColumnTypeByteSize(String colName, String colType, int size) {
        if (_columns.containsKey(colName)) {
            _columns.get(colName).setColumnTypeSize(colType, size);
        }
        else {
            _columns.put(colName, new ColumnData());
            _columns.get(colName).setColumnTypeSize(colType, size);
        }
    }

    public void setColumnTypeRatio(String colName, String colType, double ratio) {
        if (_columns.containsKey(colName)) {
            _columns.get(colName).setColumnTypeRatio(colType, ratio);
        }
        else {
            _columns.put(colName, new ColumnData());
            _columns.get(colName).setColumnTypeRatio(colType, ratio);
        }
    }

    public int getColumnMaxByteSize(String colName) {
        if (_columns.containsKey(colName)) {
            return _columns.get(colName).getMaxByteSize();
        }
        throw new IllegalArgumentException("Column " + colName + " in table " + _name + " does not exists");
    }

    public int getColumnTypeByteSize(String colName, String colType) {
        if (_columns.containsKey(colName)) {
            return _columns.get(colName).getColumnTypeByteSize(colType);
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

    public void addColumnType(String colName, String type) {
        if(_columns.containsKey(colName)) {
            _columns.get(colName).addType(type);
        }
        else {
            _columns.put(colName, new ColumnData());
            _columns.get(colName).addType(type);
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

    /**
     * Private method for converting columns to valid map to save in org.bson.Document
     * @return converted map
     */
    public Map<String, Object> _parseColumnsToMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        for (var entry : _columns.entrySet()) {
            map.put(entry.getKey(), entry.getValue().toMap());
        }
        return map;
    }

    /**
     * Method for converting TableData to map for saving in org.Bson.Document
     * @return converted map
     */
    public Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        if (_size != null)
            map.put("size", _size);
        if (_sizeInPages != null)
            map.put("sizeInPages", _sizeInPages);
        if (_rowCount != null)
            map.put("rowCount", _rowCount);
        if (_constraintCount != null)
            map.put("constraintCount", _constraintCount);
        map.put("columns", _parseColumnsToMap());
        return map;
    }
}
