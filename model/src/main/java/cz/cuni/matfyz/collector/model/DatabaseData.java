package cz.cuni.matfyz.collector.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.*;

/** Class holding statistical data about dataset */
public class DatabaseData {

    /** Field containing size of dataset in bytes */
    @JsonProperty("databaseSize")
    private Long _databaseSize;
    /** Field containing size of dataset in pages (virtual disk block size) */
    @JsonProperty("databaseSizeInPages")
    private Long _databaseSizeInPages;
    /** Field containing size of page in bytes */
    @JsonProperty("pageSize")
    private Integer _pageSize;
    /** Field containing size of caches in bytes which could be used for query caching */
    @JsonProperty("cacheSize")
    private Long _cacheSize;

    @JsonProperty("tables")
    private final HashMap<String, KindData> _kinds;
    @JsonProperty("indexes")
    private final HashMap<String, IndexData> _indexes;

    public DatabaseData() {
        _kinds = new HashMap<>();
        _indexes = new HashMap<>();

        _databaseSize = null;
        _databaseSizeInPages = null;
        _pageSize = null;
        _cacheSize = null;
    }

    // Database setting methods
    public void setDatabaseSize(long size) {
        if(_databaseSize == null)
            _databaseSize = size;
    }

    public void setDatabaseSizeInPages(long dataSetSizeInPages) {
        if (_databaseSizeInPages == null) { _databaseSizeInPages = dataSetSizeInPages; }
    }
    public void setDatabasePageSize(int pageSize) {
        if (_pageSize == null)
            _pageSize = pageSize;
    }

    @JsonIgnore
    public int getDatabasePageSize() {
        return _pageSize;
    }

    public void setDatabaseCacheSize(long size) {
        if(_cacheSize == null)
            _cacheSize = size;
    }

    @JsonIgnore
    public KindData getKind(String kindName) throws DataModelException {
        if (!_kinds.containsKey(kindName)) {
            throw new DataModelException(String.format("Kind %s does not exist in DataModel instance.", kindName));
        }
        return _kinds.get(kindName);
    }

    public DatabaseData addKindIfNeeded(String kindName) {
        if (!_kinds.containsKey(kindName)) {
            _kinds.put(kindName, new KindData(kindName));
        }
        return this;
    }

    @JsonIgnore
    public IndexData getIndex(String inxName) throws DataModelException {
        if (!_indexes.containsKey(inxName)) {
            throw new DataModelException(String.format("Index %s does not exist in DataModel instance.", inxName));
        }
        return _indexes.get(inxName);
    }

    public DatabaseData addIndexIfNeeded(String inxName) {
        if (!_indexes.containsKey(inxName)) {
            _indexes.put(inxName, new IndexData());
        }
        return this;
    }

    @JsonIgnore
    public Set<String> getKindNames() {
        return _kinds.keySet();
    }

    @JsonIgnore
    public Set<String> getIndexNames() {
        return _indexes.keySet();
    }
}
