package cz.cuni.matfyz.collector.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;

/**
 * Class holding statistical data about table
 */
public class KindData {

    @JsonIgnore
    private final String _kindName;

    /** Field containing size of table in bytes */
    @JsonProperty("byteSize")
    private Long _byteSize;

    /** Field containing size of table in pages */
    @JsonProperty("sizeInPages")
    private Long _sizeInPages;

    /** Field containing row count of table */
    @JsonProperty("recordCount")
    private Long _recordCount;

    /** Field containing number of constraints defined over table */
    @JsonProperty("constraintCount")
    private Integer _constraintCount;

    @JsonProperty("attributes")
    private final HashMap<String, AttributeData> _attributes;

    public KindData(String kindName) {
        _attributes = new HashMap<>();
        _byteSize = null;
        _sizeInPages = null;
        _recordCount = null;
        _constraintCount = null;
        _kindName = kindName;
    }

    //Tables setting methods
    public void setByteSize(long size) {
        if (_byteSize == null)
            _byteSize = size;
    }

    public void setSizeInPages(long sizeInPages) {
        if(_sizeInPages == null)
            _sizeInPages = sizeInPages;
    }

    public void setRecordCount(long count) {
        if (_recordCount == null)
            _recordCount = count;
    }

    public void setConstraintCount(int count) {
        if (_constraintCount == null) {
            _constraintCount = count;
        }
    }

    @JsonIgnore
    public AttributeData getAttribute(String attributeName) throws DataModelException {
        if (!_attributes.containsKey(attributeName)) {
            throw new DataModelException(String.format("Attribute %s.%s does not exist in DataModel instance.", _kindName, attributeName));
        }
        return _attributes.get(attributeName);
    }

    @JsonIgnore
    public KindData addAttributeIfAbsent(String attributeName) {
        if (!_attributes.containsKey(attributeName)) {
            _attributes.put(attributeName, new AttributeData(attributeName, _kindName));
        }
        return this;
    }
}
