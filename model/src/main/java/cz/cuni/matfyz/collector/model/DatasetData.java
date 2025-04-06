package cz.cuni.matfyz.collector.model;

import java.util.*;

/** Class holding statistical data about dataset */
public class DatasetData implements MapWritable {

    /** Field containing size of dataset in bytes */
    private Long _datasetSize;
    /** Field containing size of dataset in pages (virtual disk block size) */
    private Long _datasetSizeInPages;
    /** Field containing size of page in bytes */
    private Integer _pageSize;
    /** Field containing size of caches in bytes which could be used for query caching */
    private Long _cacheSize;

    private final HashMap<String, TableData> _tables;
    private final HashMap<String, IndexData> _indexes;

    public DatasetData() {
        _tables = new HashMap<>();
        _indexes = new HashMap<>();

        _datasetSize = null;
        _datasetSizeInPages = null;
        _pageSize = null;
        _cacheSize = null;
    }

    // Database setting methods
    public void setDataSetSize(long size) {
        if(_datasetSize == null)
            _datasetSize = size;
    }

    public void setDataSetSizeInPages(long dataSetSizeInPages) {
        if (_datasetSizeInPages == null) { _datasetSizeInPages = dataSetSizeInPages; }
    }
    public void setDataSetPageSize(int pageSize) {
        if (_pageSize == null)
            _pageSize = pageSize;
    }
    public int getDataSetPageSize() {
        return _pageSize;
    }

    public void setDataSetCacheSize(long size) {
        if(_cacheSize == null)
            _cacheSize = size;
    }

    public TableData getTable(String tableName, boolean createIfNotExist) throws IllegalArgumentException {
        if (!_tables.containsKey(tableName) && createIfNotExist) {
            _tables.put(tableName, new TableData());
        } else if (!_tables.containsKey(tableName) && !createIfNotExist) {
            throw new IllegalArgumentException("Table '" + tableName + "' does not exists in DataModel");
        }
        return _tables.get(tableName);
    }

    public IndexData getIndex(String inxName, boolean createIfNotExist) {
        if (!_indexes.containsKey(inxName) && createIfNotExist) {
            _indexes.put(inxName, new IndexData());
        } else if (!_indexes.containsKey(inxName) && !createIfNotExist) {
            throw new IllegalArgumentException("Index '" + inxName + "' does not exists in DataModel");
        }
        return _indexes.get(inxName);
    }

    public Set<String> getTableNames() {
        return _tables.keySet();
    }
    public void addTable(String tableName) {
        if (!_tables.containsKey(tableName)) {
            _tables.put(tableName, new TableData());
        }
    }

    public Set<String> getIndexNames() {
        return _indexes.keySet();
    }
    public void addIndex(String inxName) {
        if(!_indexes.containsKey(inxName)) {
            _indexes.put(inxName, new IndexData());
        }
    }

    public void WriteTo(Map<String, Object> map) {
        if (_datasetSize != null)
            map.put("datasetSize", _datasetSize);
        if (_datasetSizeInPages != null)
            map.put("datasetSizeInPages", _datasetSizeInPages);
        if (_pageSize != null)
            map.put("pageSize", _pageSize);
        if (_cacheSize != null)
            map.put("cacheSize", _cacheSize);
    }

    public MapWritableCollection<TableData> tables = new MapWritableCollection<TableData>() {
        @Override
        public Set<Map.Entry<String, TableData>> getItems() {
            return _tables.entrySet();
        }
        @Override
        public void AppendTo(Map<String, Object> rootMap, Map<String, Object> itemsMap) {
            rootMap.put("tables", itemsMap);
        }
        @Override
        public MapWritableCollection<ColumnData> getCollectionFor(String name) {
            return getTable(name, false);
        }
    };

    public MapWritableCollection<IndexData> indexes = new MapWritableCollection<IndexData>() {
        @Override
        public Set<Map.Entry<String, IndexData>> getItems() {
            return _indexes.entrySet();
        }
        @Override
        public void AppendTo(Map<String, Object> rootMap, Map<String, Object> itemsMap) {
            rootMap.put("indexes", itemsMap);
        }
        @Override
        public MapWritableCollection<MapWritable> getCollectionFor(String name) {
            return null;
        }
    };
}
