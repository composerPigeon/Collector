package cz.cuni.matfyz.collector.persistor;

import cz.cuni.matfyz.collector.model.DataModel;

public abstract class AbstractPersistor {
    public abstract void saveExecution(String uuid, DataModel model);

    //returns null if execution doesn't exists
    public abstract String getExecution(String uuid);
}
