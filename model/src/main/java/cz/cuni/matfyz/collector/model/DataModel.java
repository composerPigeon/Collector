package cz.cuni.matfyz.collector.model;

import java.util.Map;
import java.util.Set;

public interface DataModel {
    static DataModel CreateForQuery(String query, String systemName, String databaseName) {
        return new QueryDataModel(query, systemName, databaseName);
    }

    void setResultExecutionTime(double time);
    void setResultByteSize(long size);
    void setResultSizeInPages(long size);
    void setResultRecordCount(long count);

    void setDatabaseByteSize(long size);
    void setDatabaseSizeInPages(long size);
    void setDatabaseCacheSize(long size);
    void setPageSize(int size);
    int getPageSize();

    void setKindByteSize(String kindName, long size);
    void setKindSizeInPages(String kindName, long size);
    void setKindRecordCount(String kindName, long count);
    void setKindConstraintCount(String kindName, int count);
    void addKind(String kindName);
    Set<String> getKindNames();

    void setIndexByteSize(String indexName, long size);
    void setIndexSizeInPages(String indexName, long size);
    void setIndexRecordCount(String indexName, long count);
    void addIndex(String indexName);
    Set<String> getIndexNames();

    void setAttributeMandatory(String kindName, String attributeName, boolean mandatory);
    void setAttributeDistinctValuesCount(String kindName, String attributeName, long count);
    int getAttributeMaxByteSize(String kindName, String attributeName) throws DataModelException;

    void setAttributeTypeByteSize(String kindName, String attributeName, String typeName, int size);
    void setResultAttributeTypeByteSize(String attributeName, String typeName, int size);
    void setAttributeTypeRatio(String kindName, String attributeName, String typeName, double ratio);
    void setResultAttributeTypeRatio(String attributeName, String typeName, double ratio);
    void addAttributeType(String kindName, String attributeName, String typeName);

    int getAttributeTypeByteSize(String kindName, String attributeName, String typeName) throws DataModelException;

    String toJson() throws DataModelException;
}
