package cz.cuni.matfyz.collector.persistor;

import cz.cuni.matfyz.collector.model.DataModel;

public abstract class AbstractPersistor {
    public abstract void saveExecution(String uuid, DataModel model) throws PersistorException;

    //returns null if execution doesn't exists
    public abstract String getExecutionResult(String uuid) throws PersistorException;

    public abstract boolean getExecutionStatus(String uuid) throws PersistorException;
}
