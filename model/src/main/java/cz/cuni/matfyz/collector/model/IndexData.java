package cz.cuni.matfyz.collector.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Class for saving statistical data about index
 */
public class IndexData {
    /** Field holding index size in bytes */
    @JsonProperty("byteSize")
    public Long _byteSize;
    /** Field holding index size in pages */
    @JsonProperty("sizeInPages")
    public Long _sizeInPages;
    /** Field holding index row count */
    @JsonProperty("recordCount")
    public Long _recordCount;

    public IndexData() {
        _byteSize = null;
        _sizeInPages = null;
        _recordCount = null;
    }

    public void setByteSize(long size) {
        if (_byteSize == null) {
            _byteSize = size;}
    }

    public void setSizeInPages(long sizeInPages) {
        if (_sizeInPages == null) {_sizeInPages = sizeInPages;}
    }

    public void setRowCount(long count) {
        if (_recordCount == null) {
            _recordCount = count;}
    }
}
