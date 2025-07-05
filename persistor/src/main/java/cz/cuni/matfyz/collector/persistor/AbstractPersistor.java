package cz.cuni.matfyz.collector.persistor;

import cz.cuni.matfyz.collector.model.DataModel;

/** Class representing abstract connection to some implementation of Persistor */
public abstract class AbstractPersistor {
    /**
     * Method for saving execution result into persistor
     * @param uuid id of execution
     * @param model model of collected statistical data for this execution
     * @throws PersistorException when some exception occur during execution saving
     */
    public abstract void saveExecutionResult(String uuid, DataModel model) throws PersistorException;

    /**
     * Method for saving execution error into persistor
     * @param uuid id of execution
     * @param errMsg error message
     * @throws PersistorException when seme error occur during this procedure
     */
    public abstract void saveExecutionError(String uuid, String errMsg) throws PersistorException;

    /**
     * Method for getting execution result that is already saved in persistor
     * @param uuid id of execution
     * @return json string of DataModel or error message if execution ended with error. If execution doesn't exist then return null as a result
     * @throws PersistorException when some error occurred during this procedure
     */
    public abstract ExecutionResult getExecutionResult(String uuid) throws PersistorException;

    /**
     * Method for getting execution state that is already saved in persistor
     * @param uuid id of execution
     * @return true if execution is present in persistor
     * @throws PersistorException when some error occurred during this procedure
     */
    public abstract boolean containsExecution(String uuid) throws PersistorException;
}
