package cz.cuni.matfyz.collector.wrappers.neo4j.components;

import java.util.ArrayList;
import java.util.List;

public class IndexParseRecord {
    private final String _indexType;
    private final String _label;
    private final String[] _properties;
    private final String _indexName;

    public IndexParseRecord(String indexType, String label, String[] properties) {
        _indexType = indexType;
        _label = label;
        _properties = properties;
        _indexName = indexType + ":" + label + ":" + String.join(",", properties);
    }

    public IndexParseRecord(String indexName) {
        String[] parts = indexName.split(":");
        _indexType = parts[0];
        _label = parts[1];
        _properties = parts[2].split(",");
        _indexName = indexName;
    }

    public String getIndexType() {
        return _indexType;
    }

    public String getLabel() {
        return _label;
    }

    public String[] getProperties() {
        return _properties;
    }

    public String getIndexName() {
        return _indexName;
    }

    public static class Builder {
        private String _indexType;
        private String _label;
        private final List<String> _properties = new ArrayList<>();

        public Builder setIndexType(String indexType) {
            _indexType = indexType;
            return this;
        }

        public Builder setLabel(String label) {
            _label = label;
            return this;
        }

        public Builder addProperty(String property) {
            _properties.add(property);
            return this;
        }

        public IndexParseRecord build() {
            return new IndexParseRecord(_indexType, _label, _properties.toArray(String[]::new));
        }
    }
}
