package cz.cuni.matfyz.collector.wrappers.neo4j;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractDataCollector;
import cz.cuni.matfyz.collector.wrappers.cachedresult.CachedResult;
import cz.cuni.matfyz.collector.wrappers.exceptions.DataCollectException;
import cz.cuni.matfyz.collector.wrappers.exceptions.QueryExecutionException;
import org.neo4j.driver.Result;
import org.neo4j.driver.summary.Plan;

public class Neo4jDataCollector extends AbstractDataCollector<Plan, Result> {

    public Neo4jDataCollector(Neo4jConnection connection, DataModel model, String datasetName) {
        super(datasetName, model, connection);
    }

    private void _savePageSize() {
        _model.toDatasetData().setDataSetPageSize(Neo4jResources.DefaultSizes.PAGE_SIZE);
    }
    private void _saveDatasetSize() {

    }

    private void _saveColumnData(String TableName) {

    }

    private int[] _fetchPropertiesSize(int recordSize, String fetchQuery) throws QueryExecutionException {
        CachedResult result = _connection.executeQuery(fetchQuery);
        int propertiesSize = 0;
        while (result.next()) {
            var record = result.getRecord();
            for (var column : record.entrySet()) {
                propertiesSize += Neo4jResources.DefaultSizes.getAvgColumnSize(column.getValue());
            }
        }
        return new int[]{
                result.getRowCount() * recordSize + propertiesSize, result.getRowCount()
        };
    }

    private void _saveTableSizes(String tableName) throws QueryExecutionException {
        int[] nodeTuple = _fetchPropertiesSize(Neo4jResources.DefaultSizes.NODE_SIZE, Neo4jResources.getNodesQuery(tableName));
        int[] edgeTuple = _fetchPropertiesSize(Neo4jResources.DefaultSizes.EDGE_SIZE, Neo4jResources.getRelationsQuery(tableName));
        int size = nodeTuple[0] + edgeTuple[0];

        _model.toDatasetData().setTableByteSize(tableName, size);
        _model.toDatasetData().setTableSizeInPages(tableName, (int) Math.ceil(
                (double) size / Neo4jResources.DefaultSizes.PAGE_SIZE
        ));
        _model.toDatasetData().setTableRowCount(tableName, nodeTuple[1] + edgeTuple[1]);
    }
    private void _saveTableData() throws QueryExecutionException {
        for (String label : _model.getTableNames()) {
            _saveTableSizes(label);
            _saveColumnData(label);
        }
    }

    private void _saveIndexData() {
        //TODO: save index data
    }
    @Override
    public DataModel collectData(CachedResult result) throws DataCollectException {
        try {
            _savePageSize();
            _saveDatasetSize();
            _saveIndexData();
            _saveTableData();

            return _model;
        } catch (QueryExecutionException e) {
            throw new DataCollectException(e);
        }
    }
}
