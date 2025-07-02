package cz.cuni.matfyz.collector.model;

import java.util.Set;

public interface DataModel {
    public static DataModel CreateForQuery(String query, String systemName, String databaseName) {
        return new QueryDataModel(query, systemName, databaseName);
    }

    public void setResultExecutionTime(double time);
    public void setResultByteSize(long size);
    public void setResultSizeInPages(long size);
    public void setResultRecordCount(long count);

    public void setDatabaseByteSize(long size);
    public void setDatabaseSizeInPages(long size);
    public void setDatabaseCacheSize(long size);
    public void setPageSize(int size);
    public int getPageSize();

    public void setKindByteSize(String kindName, long size);
    public void setKindSizeInPages(String kindName, long size);
    public void setKindRecordCount(String kindName, long count);
    public void setKindConstraintCount(String kindName, int count);
    public void addKind(String kindName);
    public Set<String> getKindNames();

    public void setIndexByteSize(String indexName, long size);
    public void setIndexSizeInPages(String indexName, long size);
    public void setIndexRecordCount(String indexName, long count);
    public void addIndex(String indexName);
    public Set<String> getIndexNames();

    public void setAttributeMandatory(String kindName, String attributeName, boolean mandatory);
    public void setAttributeDistinctValuesCount(String kindName, String attributeName, long count);
    public int getAttributeMaxByteSize(String kindName, String attributeName) throws DataModelException;

    public void setAttributeTypeByteSize(String kindName, String attributeName, String typeName, int size);
    public void setResultAttributeTypeByteSize(String attributeName, String typeName, int size);
    public void setAttributeTypeRatio(String kindName, String attributeName, String typeName, double ratio);
    public void setResultAttributeTypeRatio(String attributeName, String typeName, double ratio);
    public void addAttributeType(String kindName, String attributeName, String typeName);

    public int getAttributeTypeByteSize(String kindName, String attributeName, String typeName) throws DataModelException;

    public String toJson() throws DataModelException;
}
