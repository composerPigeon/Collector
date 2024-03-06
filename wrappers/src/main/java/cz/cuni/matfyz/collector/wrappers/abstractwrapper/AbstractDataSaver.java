package cz.cuni.matfyz.collector.wrappers.abstractwrapper;

import cz.cuni.matfyz.collector.model.DataModel;

public abstract class AbstractDataSaver {
    protected String _datasetName;

    public AbstractDataSaver(String datasetName) {
        _datasetName = datasetName;
    }

    public abstract void saveDataTo(DataModel dataModel) throws DataSaveException;
}
