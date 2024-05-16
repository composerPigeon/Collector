package cz.cuni.matfyz.collector.wrappers.cachedresult;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ConsumedResult {

    private final Map<String, String> _columnTypes;
    private final long _byteSize;
    private final long _count;

    public ConsumedResult(Map<String, String> columnmTypes, long byteSize, long count) {
        _byteSize = byteSize;
        _count = count;
        _columnTypes = columnmTypes;
    }

    public Set<String> getColumnNames() {
        return _columnTypes.keySet();
    }

    public String getColumnType(String colName) {
        return _columnTypes.get(colName);
    }

    public long getByteSize() {
        return _byteSize;
    }
    public long getRowCount() {
        return _count;
    }

    public static class Builder {

        private Map<String, String> _columnTypes;
        private long _byteSize;
        private long _count;

        public Builder() {
            _columnTypes = new HashMap<>();
            _count = 0;
            _byteSize = 0;
        }

        public void addColumnType(String colName, String type) {
            if (!_columnTypes.containsKey(colName))
                _columnTypes.put(colName, type);
        }

        public boolean containsTypeForCol(String colName) {
            return _columnTypes.containsKey(colName);
        }

        public void addRecord() {
            _count += 1;
        }

        public void addByteSize(int size) {
            _byteSize += size;
        }

        public ConsumedResult toResult() {
            return new ConsumedResult(
                    _columnTypes,
                    _byteSize,
                    _count
            );
        }
    }

}
