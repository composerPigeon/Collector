package cz.cuni.matfyz.collector.wrappers.postgresql;

import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractWrapper;
import cz.cuni.matfyz.collector.wrappers.exceptions.DataCollectException;
import cz.cuni.matfyz.collector.wrappers.exceptions.WrapperExceptionsFactory;

public class PostgresExceptionsFactory extends WrapperExceptionsFactory {
    public PostgresExceptionsFactory(AbstractWrapper.ConnectionData connectionData) {
        super(connectionData);
    }

    public DataCollectException tableForColumnNotFound(String columnName) {
        var message = new Message("no table for column '" + columnName + "' was found").toString();
        return new DataCollectException(message);
    }

    public DataCollectException byteSizeForColumnTypeNotFoundInDataModel(String columnName, String columnType) {
        var message = new Message(String.format("No byte size for column '%s' of type '%s' was found in data model", columnName, columnType)).toString();
        return new DataCollectException(message);
    }
}
