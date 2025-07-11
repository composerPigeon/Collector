package cz.cuni.matfyz.collector.wrappers.mongodb;

import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractWrapper;
import cz.cuni.matfyz.collector.wrappers.exceptions.DataCollectException;
import cz.cuni.matfyz.collector.wrappers.exceptions.ParseException;
import cz.cuni.matfyz.collector.wrappers.exceptions.WrapperExceptionsFactory;
import cz.cuni.matfyz.collector.wrappers.mongodb.queryparser.CommandBuilder;

public class
MongoExceptionsFactory extends WrapperExceptionsFactory {
    public MongoExceptionsFactory(AbstractWrapper.ConnectionData connectionData) {
        super(connectionData);
    }

    //region ParseException initialization
    public ParseException documentKeyNotFound(String key) {
        var message = new MessageBuilder()
                .withContent("Key '%s' was not present in document", key)
                .build();
        return new ParseException(message);
    }

    public ParseException invalidNumberOfArgumentsInMethod(String methodName, CommandBuilder.ReturnType type) {
        var message = new MessageBuilder()
                .withContent(
                        "Method '%s' called on type '%s' has invalid number of arguments. At most two are expected to be present",
                        methodName,
                        type.name().toLowerCase()
                ).build();
        return new ParseException(message);
    }

    public ParseException notSupportedMethod(String methodName, CommandBuilder.ReturnType type) {
        var message = new MessageBuilder()
                .withContent(
                        "Method '%s' called on type '%s' is not supported by system",
                        methodName,
                        type.name().toLowerCase()
                ).build();
        return new ParseException(message);
    }

    public ParseException invalidMethod(String methodName, CommandBuilder.ReturnType type) {
        var message = new MessageBuilder()
                .withContent(
                        "Method '%s' called on type '%s' is does not exist",
                        methodName, type.name().toLowerCase()
                ).build();
        return new ParseException(message);
    }

    public ParseException invalidMethodOption(String optionName, String methodName, CommandBuilder.ReturnType type) {
        var message = new MessageBuilder()
                .withContent(
                        "Option '%s' in method '%s' called on type '%s' is not supported or does not exist",
                        optionName,
                        methodName,
                        type.name().toLowerCase()
                ).build();
        return new ParseException(message);
    }

    public ParseException invalidCountUsage() {
        var message = new MessageBuilder()
                .withContent("Method 'count' can be used on result of 'find' method only")
                .build();
        return new ParseException(message);
    }
    //endregion

    public DataCollectException collectionNotParsed() {
        var message = new MessageBuilder()
                .withContent("No collection was parsed from explain plan")
                .build();
        return new DataCollectException(message);
    }
}
