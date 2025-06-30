package cz.cuni.matfyz.collector.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class QueryDataModel implements DataModel {
    private final QueryData _query;

    private static final Logger logger = LoggerFactory.getLogger(QueryDataModel.class);

    public QueryDataModel(String query, String databaseName, String datasetName) {
        _query = new QueryData(query, databaseName, datasetName);
    }

    //ResultData
    @Override
    public void setResultExecutionTime(double time) { _query.getResultData().setExecutionTime(time); }

    @Override
    public void setResultByteSize(long size) { _query.getResultData().getResultKind().setByteSize(size); }

    @Override
    public void setResultSizeInPages(long size) { _query.getResultData().getResultKind().setSizeInPages(size); }

    @Override
    public void setResultRecordCount(long count) { _query.getResultData().getResultKind().setRecordCount(count); }

    //DatasetData
    @Override
    public void setDatabaseByteSize(long size) { _query.getDatabaseData().setDatabaseSize(size); }
    @Override
    public void setDatabaseSizeInPages(long size) { _query.getDatabaseData().setDatabaseSizeInPages(size); }
    @Override
    public void setDatabaseCacheSize(long size) { _query.getDatabaseData().setDatabaseCacheSize(size); }
    @Override
    public void setPageSize(int size) { _query.getDatabaseData().setDatabasePageSize(size); }
    @Override
    public int getPageSize() { return _query.getDatabaseData().getDatabasePageSize(); }

    //KindData
    @Override
    public void setKindByteSize(String kindName, long size) {
        try {
            _query.getDatabaseData()
                    .addKindIfAbsent(kindName)
                    .getKind(kindName)
                    .setByteSize(size);
        } catch (DataModelException e) {
            logger.atError().setCause(e).log(e.getMessage());
        }
    }
    @Override
    public void setKindSizeInPages(String kindName, long size) {
        try {
            _query.getDatabaseData()
                    .addKindIfAbsent(kindName)
                    .getKind(kindName)
                    .setSizeInPages(size);
        } catch (DataModelException e) {
            logger.atError().setCause(e).log(e.getMessage());
        }
    }
    @Override
    public void setKindRecordCount(String kindName, long count) {
        try {
            _query.getDatabaseData()
                    .addKindIfAbsent(kindName)
                    .getKind(kindName)
                    .setRecordCount(count);
        } catch (DataModelException e) {
            logger.atError().setCause(e).log(e.getMessage());
        }
    }
    @Override
    public void setKindConstraintCount(String kindName, int count) {
        try {
            _query.getDatabaseData()
                    .addKindIfAbsent(kindName)
                    .getKind(kindName)
                    .setConstraintCount(count);
        } catch (DataModelException e) {
            logger.atError().setCause(e).log(e.getMessage());
        }
    }
    @Override
    public void addKind(String tableName) { _query.getDatabaseData().addKindIfAbsent(tableName); }
    @Override
    public Set<String> getKindNames() { return _query.getDatabaseData().getKindNames(); }

    //IndexData
    @Override
    public void setIndexByteSize(String indexName, long size) {
        try {
            _query.getDatabaseData()
                    .addIndexIfAbsent(indexName)
                    .getIndex(indexName)
                    .setByteSize(size);
        } catch (DataModelException e) {
            logger.atError().log(e.getMessage());
        }
    }
    @Override
    public void setIndexSizeInPages(String indexName, long size) {
        try {
            _query.getDatabaseData()
                    .addIndexIfAbsent(indexName)
                    .getIndex(indexName)
                    .setSizeInPages(size);
        } catch (DataModelException e) {
            logger.atError().log(e.getMessage());
        }
    }
    @Override
    public void setIndexRecordCount(String indexName, long count) {
        try {
            _query.getDatabaseData()
                    .addIndexIfAbsent(indexName)
                    .getIndex(indexName)
                    .setRowCount(count);
        } catch (DataModelException e) {
            logger.atError().setCause(e).log(e.getMessage());
        }
    }
    @Override
    public void addIndex(String indexName) { _query.getDatabaseData().addIndexIfAbsent(indexName); }
    @Override
    public Set<String> getIndexNames() { return _query.getDatabaseData().getIndexNames(); }

    //AttributeData
    @Override
    public void setAttributeMandatory(String kindName, String attributeName, boolean mandatory) {
        try {
            _query.getDatabaseData()
                    .addKindIfAbsent(kindName)
                    .getKind(kindName)
                    .addAttributeIfNeeded(attributeName)
                    .getAttribute(attributeName)
                    .setMandatory(mandatory);
        } catch (DataModelException e) {
            logger.atError().setCause(e).log(e.getMessage());
        }
    }
    @Override
    public void setAttributeValueRatio(String kindName, String attributeName, double ratio) {
        try {
            _query.getDatabaseData()
                    .addKindIfAbsent(kindName)
                    .getKind(kindName)
                    .addAttributeIfNeeded(attributeName)
                    .getAttribute(attributeName)
                    .setDistinctRatio(ratio);
        } catch (DataModelException e) {
            logger.atError().setCause(e).log(e.getMessage());
        }
    }
    @Override
    public int getAttributeMaxByteSize(String kindName, String attributeName) throws DataModelException {
        return _query.getDatabaseData()
                .getKind(kindName)
                .getAttribute(attributeName)
                .getMaxByteSize();
    }

    //AttributeTypeData
    @Override
    public void setAttributeTypeByteSize(String kindName, String attributeName, String typeName, int size) {
        try {
            _query.getDatabaseData()
                    .addKindIfAbsent(kindName)
                    .getKind(kindName)
                    .addAttributeIfNeeded(attributeName)
                    .getAttribute(attributeName)
                    .addAttributeTypeIfAbsent(typeName)
                    .getAttributeType(typeName)
                    .setByteSize(size);
        } catch (DataModelException e) {
            logger.atError().setCause(e).log(e.getMessage());
        }
    }
    @Override
    public void setResultAttributeTypeByteSize(String attributeName, String typeName, int size) {
        try {
            _query.getResultData()
                    .getResultKind()
                    .addAttributeIfNeeded(attributeName)
                    .getAttribute(attributeName)
                    .addAttributeTypeIfAbsent(typeName)
                    .getAttributeType(typeName)
                    .setByteSize(size);
        } catch (DataModelException e) {
            logger.atError().setCause(e).log(e.getMessage());
        }
    }
    @Override
    public void setAttributeTypeRatio(String kindName, String attributeName, String typeName, double ratio) {
        try {
            _query.getDatabaseData()
                    .addKindIfAbsent(kindName)
                    .getKind(kindName)
                    .addAttributeIfNeeded(attributeName)
                    .getAttribute(attributeName)
                    .addAttributeTypeIfAbsent(typeName)
                    .getAttributeType(typeName)
                    .setRatio(ratio);
        } catch (DataModelException e) {
            logger.atError().setCause(e).log(e.getMessage());
        }
    }
    @Override
    public void setResultAttributeTypeRatio(String attributeName, String typeName, double ratio) {
        try {
            _query.getResultData()
                    .getResultKind()
                    .addAttributeIfNeeded(attributeName)
                    .getAttribute(attributeName)
                    .addAttributeTypeIfAbsent(typeName)
                    .getAttributeType(typeName)
                    .setRatio(ratio);
        } catch (DataModelException e) {
            logger.atError().setCause(e).log(e.getMessage());
        }
    }
    @Override
    public void addAttributeType(String kindName, String attributeName, String typeName) {
        try {
            _query.getDatabaseData()
                    .addKindIfAbsent(kindName)
                    .getKind(kindName)
                    .addAttributeIfNeeded(attributeName)
                    .getAttribute(attributeName)
                    .addAttributeTypeIfAbsent(typeName);
        } catch (DataModelException e) {
            logger.atError().setCause(e).log(e.getMessage());
        }
    }
    @Override
    public int getAttributeTypeByteSize(String kindName, String attributeName, String typeName) throws DataModelException {
        return _query.getDatabaseData()
                .getKind(kindName)
                .getAttribute(attributeName)
                .getAttributeType(typeName)
                .getByteSize();
    }

    @Override
    public String toJson() throws DataModelException {
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.writerWithDefaultPrettyPrinter();
            mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
            return mapper.writeValueAsString(_query);
        } catch (JsonProcessingException e) {
            throw new DataModelException("Problem with parsing DataModel instance to json format", e);
        }
    }
}

