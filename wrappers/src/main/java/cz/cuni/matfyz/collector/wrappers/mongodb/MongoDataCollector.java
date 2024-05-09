package cz.cuni.matfyz.collector.wrappers.mongodb;

import com.mongodb.client.MongoDatabase;
import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractDataCollector;
import cz.cuni.matfyz.collector.wrappers.cachedresult.CachedResult;
import cz.cuni.matfyz.collector.wrappers.exceptions.DataCollectException;
import cz.cuni.matfyz.collector.wrappers.exceptions.QueryExecutionException;
import org.bson.Document;

import javax.xml.crypto.Data;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

public class MongoDataCollector extends AbstractDataCollector<Document, Document, Document> {

    public MongoDataCollector(MongoConnection connection, DataModel model, String datasetName) {
        super(datasetName, model, connection);
    }

    private void _savePageSize(String collectionName) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(MongoResources.getCollectionStatsCommand(collectionName));

        if (result.next()) {
            int pageSize = result.getDocument("wiredTiger").get("block-manager", Document.class).getInteger("file allocation unit size");
            _model.datasetData().setDataSetPageSize(pageSize);
        }
    }

    private void _saveDatasetData() throws QueryExecutionException {
        CachedResult stats = _connection.executeQuery(MongoResources.getDatasetStatsCommand());

        if (stats.next()) {
            long size = stats.getLong("storageSize");
            _model.datasetData().setDataSetSize(size);
            long sizeInPages = (long) Math.ceil((double)size / _model.getPageSize());
            _model.datasetData().setDataSetSizeInPages(sizeInPages);
        }
    }

    private void _saveTableData(String collectionName) throws QueryExecutionException {
        CachedResult stats = _connection.executeQuery(MongoResources.getCollectionStatsCommand(collectionName));

        if (stats.next()) {
            long size = stats.getLong("storageSize");
            _model.datasetData().setTableByteSize(collectionName, size);
            long sizeInPages = (long) Math.ceil((double)size / _model.getPageSize());
            _model.datasetData().setTableSizeInPages(collectionName, sizeInPages);

            long rowCount = stats.getLong("count");
            _model.datasetData().setTableRowCount(collectionName, rowCount);
        }
    }

    // saves index row count if it doesn't correspond to collection count
    private void _saveIndexRowCount(String collectionName, String indexName) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(MongoResources.getIndexRowCountCommand(collectionName, indexName));
    }

    private void _saveIndexData(String collectionName, String indexName) throws QueryExecutionException {
        CachedResult stats = _connection.executeQuery(MongoResources.getCollectionWithIndexesStatsCommand(collectionName));
        if (stats.next()) {
            long size = stats.getDocument("indexSizes").getLong(indexName);
            _model.datasetData().setIndexByteSize(indexName, size);
            _model.datasetData().setIndexSizeInPages(indexName, (long)Math.ceil((double)size / _model.getPageSize()));

            stats.getDocument("indexDetails").get(indexName, Document.class); //TODO: get index type
            //TODO: check index type
            if (true) {
                long collectionCount = stats.getLong("count");
                _model.datasetData().setIndexRowCount(indexName, collectionCount);
            } else {
                _saveIndexRowCount(collectionName, indexName);
            }
        }
    }

    private void _saveIndexesData(String collectionName) throws QueryExecutionException {
        for (String indexName : _model.getIndexNames()) {
            _saveIndexData(collectionName, indexName);
        }
    }

    private String _getCollectionName() throws DataCollectException {
        for (String collectionName : _model.getTableNames()) {
            return collectionName;
        }
        throw new DataCollectException("No collection parsed from query");
    }

    @Override
    public DataModel collectData(CachedResult result) throws DataCollectException {
        try {
            String collName = _getCollectionName();
            _savePageSize(collName);
            _saveDatasetData();
            _saveIndexesData(collName);
            _saveTableData(collName);
            return _model;
        } catch (QueryExecutionException e) {
            throw new DataCollectException(e);
        }
    }
}
