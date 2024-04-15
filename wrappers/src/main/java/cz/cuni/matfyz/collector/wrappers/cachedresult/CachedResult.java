package cz.cuni.matfyz.collector.wrappers.cachedresult;

import java.util.*;

public class CachedResult {
    private final List<Map<String, Object>> _records;
    private final int _byteSize;
    private int _cursor;

    protected CachedResult(List<Map<String, Object>> records, int byteSize) {
        _records = records;
        _byteSize = byteSize;
        _cursor = -1;
    }

    public boolean next() {
        _cursor++;
        return _cursor < _records.size();
    }

    public void refresh() { _cursor = -1; }
    public Map<String, Object> getRecord() { return _records.get(_cursor); }

    public int getInt(String colName) {
        Object value = _records.get(_cursor).get(colName);
        if (value instanceof Integer intValue) {
            return intValue;
        }
        else if (value instanceof String strValue){
            return Integer.parseInt(strValue);
        }
        else {
            throw new ClassCastException();
        }
    }
    public String getString(String colName) {
        return (String)_records.get(_cursor).get(colName);
    }
    public double getDouble(String colName) {
        return (Double)_records.get(_cursor).get(colName);
    }

    public int getRowCount() {
        return _records.size();
    }
    public int getByteSize() { return _byteSize; }

    public static class Builder {
        private final List<Map<String, Object>> _records;
        protected int _byteSize;
        public Builder() {
            _records = new ArrayList<>();
            _byteSize = 0;
        }
        public void addEmptyRecord() {
            _records.add(new LinkedHashMap<>());
        }
        public void addSize(int value) { _byteSize += value; }

        public void toLastRecordAddValue(String colName, Object value) {
            int lastInx = _records.size() - 1;
            _records.get(lastInx).put(colName, value);
        }
        public CachedResult toResult() {
            return new CachedResult(_records, _byteSize);
        }
    }
}
