package cz.cuni.matfyz.collector.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.*;

/**
 * Class responsible for representing collected statistical data about individual columns
 */
public class AttributeData {
    @JsonIgnore
    private final String _attributeName;
    @JsonIgnore
    private final String _kindName;

    /**
     * Field holding information about statistical distribution of values. In PostgreSQL it holds ratio of distinct values.
     */
    @JsonProperty("ratio")
    private Double _valuesRatio;

    /**
     * Field holding dominant data type of column.
     */
    @JsonProperty("types")
    private final HashMap<String, AttributeType> _types;

    /**
     * Field holding information if column is mandatory to be set for entity in database.
     */
    @JsonProperty("mandatory")
    private Boolean _mandatory;

    public AttributeData(String attributeName, String kindName) {
        _attributeName = attributeName;
        _kindName = kindName;
        _valuesRatio = null;
        _mandatory = null;
        _types = new HashMap<>();
    }

    /**
     * Getter for Field _size
     *
     * @return value stored in _size field
     */
    @JsonIgnore
    public int getMaxByteSize() {
        return _types.values().stream().map(AttributeType::getByteSize).max(Integer::compareTo).orElse(0);
    }

    private String getAttributeTypeIdentifier(String typeName) {
        return _kindName + "." + _attributeName + "." + typeName;
    }

    @JsonIgnore
    public AttributeType getAttributeType(String typeName) throws DataModelException{
        if (!_types.containsKey(typeName)) {
            throw new DataModelException(String.format("AttributeType '%s.%s.%s' does not exist in DataModel instance.", _kindName, _attributeName, typeName));
        }
        return _types.get(typeName);
    }

    public AttributeData addAttributeTypeIfNeeded(String typeName) {
        if (!_types.containsKey(typeName)) {
            _types.put(typeName, new AttributeType());
        }
        return this;
    }

    public void setMandatory(boolean value) {
        if (_mandatory == null)
            _mandatory = value;
    }

    public void setDistinctRatio(double ratio) {
        if (_valuesRatio == null) {
            _valuesRatio = ratio;}
    }
}
