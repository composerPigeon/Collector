package cz.cuni.matfyz.collector.wrappers.cachedresult;

import org.bson.Document;

import java.util.*;
import java.util.Map.Entry;

public class CachedResult {
    private final List<Map<String, Object>> _records;
    private int _cursor;

    protected CachedResult(List<Map<String, Object>> records) {
        _records = records;
        _cursor = -1;
    }

    public boolean next() {
        _cursor++;
        return _cursor < _records.size();
    }

    public void refresh() { _cursor = -1; }
    public Map<String, Object> getRecord() { return _records.get(_cursor); }

    public boolean containsCol(String colName) {
        return _records.get(_cursor).containsKey(colName);
    }

    private Object _get(String colName) {
        return _records.get(_cursor).getOrDefault(colName, null);
    }

    public int getInt(String colName) {
        Object value = _get(colName);
        if (value == null) {
            throw new ClassCastException("Cannot cast null to int");
        }
        else if (value instanceof Integer intValue) {
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
        return (String)_get(colName);
    }
    public double getDouble(String colName) {
        Object value = _get(colName);
        if (value == null) {
            throw new ClassCastException("Cannot cast null to double");
        }
        else if (value instanceof Double doubleValue) {
            return doubleValue;
        }
        else if (value instanceof String strValue){
            return Double.parseDouble(strValue);
        }
        else {
            throw new ClassCastException();
        }
    }
    public boolean getBoolean(String colName) {
        Object value = _get(colName);
        if (value == null) {
            throw new ClassCastException("Cannot cast null to boolean");
        } else if (value instanceof Boolean booleanValue) {
            return booleanValue;
        } else if (value instanceof String strValue) {
            return Boolean.parseBoolean(strValue);
        } else {
            throw new ClassCastException();
        }
    }
    public long getLong(String colName) {
        Object value = _get(colName);
        if (value == null) {
            throw  new ClassCastException("Cannot cast null to long");
        } else if (value instanceof Long longValue) {
            return longValue;
        } else if (value instanceof Integer intValue) {
            return intValue;
        } else if (value instanceof Double doubleValue) {
            return Math.round(doubleValue);
        } else if (value instanceof String strValue) {
            return Long.parseLong(strValue);
        } else {
            throw new ClassCastException();
        }
    }

    private Map<String, Object> _parseToStringMap(Map<?, ?> objectMap) {
        Map<String, Object> stringMap = new HashMap<>();
        for (Entry<?, ?> entry : objectMap.entrySet()) {
            if (entry.getKey() instanceof String strValue) {
                stringMap.put(strValue, entry.getValue());
            }
        }
        return stringMap;
    }

    public Document getDocument(String colName) {
        Object value = _get(colName);
        if (value == null) {
            throw new ClassCastException("Cannot cast null to Document");
        } else if (value instanceof Map<?, ?> mapValue) {
            Map<String, Object> stringMap = _parseToStringMap(mapValue);
            return new Document(stringMap);
        } else {
            throw new ClassCastException();
        }
    }

    private <T> List<T> _convertList(List<?> listValue, Class<T> clazz) {
        List<T> convertedList = new ArrayList<>();
        for (Object item : listValue) {
            convertedList.add((T)item);
        }
        return convertedList;
    }

    public <T> List<T> getList(String columnName, Class<T> clazz) {
        Object value = _get(columnName);
        if (value instanceof List<?> listValue) {
            return _convertList(listValue, clazz);
        }
        throw new ClassCastException();
    }

    public int getRowCount() {
        return _records.size();
    }

    public static class Builder {
        private final List<Map<String, Object>> _records;


        public Builder() {
            _records = new ArrayList<>();
        }
        public void addEmptyRecord() {
            _records.add(new LinkedHashMap<>());
        }

        public void toLastRecordAddValue(String colName, Object value) {
            int lastInx = _records.size() - 1;
            _records.get(lastInx).put(colName, value);
        }
        public CachedResult toResult() {
            return new CachedResult(_records);
        }
    }
}
