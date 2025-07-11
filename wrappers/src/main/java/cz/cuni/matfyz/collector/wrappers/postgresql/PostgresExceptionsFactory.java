package cz.cuni.matfyz.collector.wrappers.postgresql;

import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractWrapper;
import cz.cuni.matfyz.collector.wrappers.exceptions.DataCollectException;
import cz.cuni.matfyz.collector.wrappers.exceptions.WrapperExceptionsFactory;

public class PostgresExceptionsFactory extends WrapperExceptionsFactory {
    public PostgresExceptionsFactory(AbstractWrapper.ConnectionData connectionData) {
        super(connectionData);
    }

    public DataCollectException tableForColumnNotFound(String columnName) {
        var message = new MessageBuilder()
                .withContent("No table for column '%s' was found", columnName)
                .build();
        return new DataCollectException(message);
    }

    public DataCollectException byteSizeForColumnTypeNotFoundInDataModel(String columnName, String columnType) {
        var message = new MessageBuilder()
                .withContent("No byte size for column '%s' of type '%s' was found in data model", columnName, columnType)
                .build();
        return new DataCollectException(message);
    }
}
