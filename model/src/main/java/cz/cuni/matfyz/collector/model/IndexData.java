package cz.cuni.matfyz.collector.model;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Class for saving statistical data about index
 */
public class IndexData implements Mappable<String, Object> {
    /** Field holding index size in bytes */
    public Long _size;
    /** Field holding index size in pages */
    public Long _sizeInPages;
    /** Field holding index row count */
    public Long _rowCount;

    public IndexData() {
        _size = null;
        _sizeInPages = null;
        _rowCount = null;
    }

    public void setByteSize(long size) {
        if (_size == null) {_size = size;}
    }

    public void setSizeInPages(long sizeInPages) {
        if (_sizeInPages == null) {_sizeInPages = sizeInPages;}
    }

    public void setRowCount(long count) {
        if (_rowCount == null) {_rowCount = count;}
    }

    /**
     * Method for converting IndexData to Map to use in org.bson.Document
     * @return converted map
     */
    public Map<String, Object> toMap() {
        Map<String, Object> result = new LinkedHashMap<>();
        if (_size != null)
            result.put("size", _size);
        if (_sizeInPages != null)
            result.put("sizeInPages", _sizeInPages);
        if (_rowCount != null)
            result.put("rowCount", _rowCount);
        return result;
    }
}
