package cz.cuni.matfyz.collector.model;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Class holding statistical data about table
 */
public class TableData {

    /** Field containing size of table in bytes */
    public Long _size;
    /** Field containing size of table in pages */
    public Long _sizeInPages;
    /** Field containing row count of table */
    public Long _rowCount;
    /** Field containing number of constraints defined over table */
    public Long _constraintCount;

    private final HashMap<String, ColumnData> _columns;

    public TableData() {
        _columns = new HashMap<>();
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

    public ColumnData getColumn(String columnName, boolean createIfNotExist) throws IllegalArgumentException {
        if (!_columns.containsKey(columnName) && createIfNotExist) {
            _columns.put(columnName, new ColumnData());
        } else if (!_columns.containsKey(columnName) && !createIfNotExist) {
            throw new IllegalArgumentException("Column '" + columnName + "' does not exist");
        }
        return _columns.get(columnName);
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
