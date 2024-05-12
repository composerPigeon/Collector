package cz.cuni.matfyz.collector.model;

import java.util.LinkedHashMap;
import java.util.Map;

public class IndexData {
    public Long _size; //size of table in bytes
    public Long _sizeInPages; //size of tables in pages on disk
    public Long _rowCount; //number of rows

    public IndexData() {
        _size = null;
        _sizeInPages = null;
        _rowCount = null;
    }

    //Tables setting methods
    public void setByteSize(long size) {
        if (_size == null) {_size = size;}
    }

    public void setSizeInPages(long sizeInPages) {
        if (_sizeInPages == null) {_sizeInPages = sizeInPages;}
    }

    public void setRowCount(long count) {
        if (_rowCount == null) {_rowCount = count;}
    }

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
