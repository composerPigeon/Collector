package cz.cuni.matfyz.collector.wrappers.abstractwrapper;

import cz.cuni.matfyz.collector.model.DataModel;
import cz.cuni.matfyz.collector.wrappers.exceptions.WrapperException;

/**
 * Class which represents unified API for communication with all wrappers from server module
 */
public interface Wrapper {


    /**
     * Method which is executed by QueryScheduler to compute statistical result of query over this wrapper
     * @param query inputted
     * @return instance of DataModel which contains all measured data
     * @throws WrapperException when some problem occur during process, message of this exception is saved as a result to execution if some error is thrown during evaluation
     */
    public abstract DataModel executeQuery(String query) throws WrapperException;
}
