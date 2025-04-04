package cz.cuni.matfyz.collector.model;

import java.util.*;

/**
 * Class responsible for representing collected statistical data about individual columns
 */
public class ColumnData {

    /**
     * Field holding information about statistical distribution of values. In PostgreSQL it holds ratio of distinct values.
     */
    private Double _ratio;

    /**
     * Field holding dominant data type of column.
     */
    private final HashMap<String, ColumnType> _types;

    /**
     * Field holding information if column is mandatory to be set for entity in database.
     */
    private Boolean _mandatory;

    public ColumnData() {
        _ratio = null;
        _mandatory = null;
        _types = new HashMap<>();
    }

    /**
     * Getter for Field _size
     *
     * @return value stored in _size field
     */
    public int getMaxByteSize() {
        return _types.values().stream().map(ColumnType::getByteSize).max(Integer::compareTo).orElse(0);
    }

    /**
     * Add new column type
     * @param columnType to be added
     */
    public void addType(String columnType) {
        if (!_types.containsKey(columnType))
            _types.put(columnType, new ColumnType());
    }

    public ColumnType getColumnType(String columnType, boolean createNew) {
        if (!_types.containsKey(columnType) && createNew) {
            _types.put(columnType, new ColumnType());
        } else if (!_types.containsKey(columnType) && !createNew) {
            throw new IllegalArgumentException("Column '" + columnType + "' does not exists in DataModel");
        }
        return _types.get(columnType);
    }

    public void setMandatory(boolean value) {
        if (_mandatory == null)
            _mandatory = value;
    }

    public void setDistinctRatio(double ratio) {
        if (_ratio == null) {_ratio = ratio;}
    }

    private Map<String, Object> _parseColumnTypesToMap() {
        Map<String, Object> map = new HashMap<>();
        for (var entry : _types.entrySet()) {
            map.put(entry.getKey(), entry.getValue().toMap());
        }
        return map;
    }

    /**
     * Method converting ColumnData object to map for saving it as part of org.bson.Document
     * @return converted map
     */
    public Map<String, Object> toMap() {
        var map = new LinkedHashMap<String, Object>();
        if (_ratio != null)
            map.put("ratio", _ratio);
        if (_mandatory != null)
            map.put("mandatory", _mandatory);
        map.put("types", _parseColumnTypesToMap());
        return map;
    }

}
