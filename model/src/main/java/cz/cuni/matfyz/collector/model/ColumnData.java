package cz.cuni.matfyz.collector.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.*;

/**
 * Class responsible for representing collected statistical data about individual columns
 */
public class ColumnData implements Mappable<String, Object> {

    /**
     * Field holding information about statistical distribution of values. In PostgreSQL it holds ratio of distinct values.
     */
    private Double _ratio;

    /**
     * Field holding dominant data type of column.
     */
    private final HashMap<String, ColumnType> _types;

    @JsonIgnore
    private Integer _maxByteSize;

    /**
     * Field holding information if column is mandatory to be set for entity in database.
     */
    private Boolean _mandatory;

    public ColumnData() {
        _ratio = null;
        _mandatory = null;
        _maxByteSize = null;
        _types = new HashMap<>();
    }

    /**
     * Getter for Field _size
     *
     * @return value stored in _size field
     */
    @JsonIgnore
    public int getMaxByteSize() {
        return _maxByteSize;
    }

    /**
     * Set byteSize for type in _types for this column
     * @param columnType for which type
     * @param size byte size to be set
     */
    public void setColumnTypeSize(String columnType, int size) {
        if (_maxByteSize == null || size > _maxByteSize)
            _maxByteSize = size;

        if (_types.containsKey(columnType))
            _types.get(columnType).setByteSize(size);
        else {
            _types.put(columnType, new ColumnType(columnType));
            _types.get(columnType).setByteSize(size);
        }

    }

    /**
     * Add new column type
     * @param columnType to be added
     */
    public void addType(String columnType) {
        if (!_types.containsKey(columnType))
            _types.put(columnType, new ColumnType(columnType));
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
        Map<String, Object> map = new LinkedHashMap<>();
        if (_ratio != null)
            map.put("ratio", _ratio);
        if (_mandatory != null)
            map.put("mandatory", _mandatory);
        map.put("types", _parseColumnTypesToMap());
        return map;
    }

}
