package cz.cuni.matfyz.collector.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Class responsible for representing collected statistical data about individual columns
 */
public class ColumnData {

    /**
     * Field holding avarage byte size of column.
     */
    private Integer _size;

    /**
     * Field holding information about statistical distribution of values. In PostgreSQL it holds ratio of distinct values.
     */
    private Double _ratio;

    /**
     * Field holding dominant data type of column.
     */
    private String _type;

    /**
     * Field holding information if column is mandatory to be set for entity in database.
     */
    private Boolean _mandatory;

    public ColumnData() {
        _size = null;
        _ratio = null;
        _type = null;
        _mandatory = null;
    }

    /**
     * Setter for Field _size
     *
     * @param size value to be set
     */
    public void setByteSize(int size) {
        if (_size == null)
            _size = size;
    }

    /**
     * Getter for Field _size
     *
     * @return value stored in _size field
     */
    @JsonIgnore
    public int getByteSize() {
        return _size;
    }

    /**
     * Setter for field _type
     * @param type value to be set
     */
    public void setType(String type) {
        if (_type == null)
            _type = type;
    }


    public void setMandatory(boolean value) {
        if (_mandatory == null)
            _mandatory = value;
    }

    public void setDistinctRatio(double ratio) {
        if (_ratio == null) {_ratio = ratio;}
    }

    /**
     * Method converting ColumnData object to map for saving it as part of org.bson.Document
     * @return converted map
     */
    public Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        if (_size != null)
            map.put("size", _size);
        if (_ratio != null)
            map.put("ratio", _ratio);
        if (_type != null)
            map.put("type", _type);
        if (_mandatory != null)
            map.put("mandatory", _mandatory);
        return map;
    }

}
