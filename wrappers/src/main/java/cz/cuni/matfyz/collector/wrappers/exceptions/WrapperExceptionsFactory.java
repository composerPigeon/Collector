package cz.cuni.matfyz.collector.wrappers.exceptions;

import cz.cuni.matfyz.collector.wrappers.abstractwrapper.AbstractWrapper;

public class WrapperExceptionsFactory {
    protected final AbstractWrapper.ConnectionData _connectionData;

    public WrapperExceptionsFactory(AbstractWrapper.ConnectionData connectionData) {
        _connectionData = connectionData;
    }

    //region ConnectionExceptions initialization
    public ConnectionException connectionIsNull() {
        var message = new MessageBuilder()
                .withContent("Connection is null")
                .build();
        return new ConnectionException(message);
    }

    public ConnectionException connectionNotOpen() {
        var message = new MessageBuilder()
                .withContent("Connection is not open")
                .build();
        return new ConnectionException(message);
    }

    public ConnectionException connectionNotInitialized(Throwable cause) {
        var message = new MessageBuilder()
                .withContent("Connection can not be initialized")
                .withCause(cause)
                .build();
        return new ConnectionException(message, cause);
    }
    //endregion

    //region QueryExecutionExceptions initialization
    public QueryExecutionException queryExecutionFailed(String query, Throwable cause) {
        var message = new MessageBuilder()
                .withContent("Query '%s' execution failed", query)
                .withCause(cause)
                .build();
        return new QueryExecutionException(message, cause);
    }

    public QueryExecutionException queryExecutionWithExplainFailed(String query, Throwable cause) {
        var message = new MessageBuilder()
                .withContent("Query '%s' execution with explain failed", query)
                .withCause(cause)
                .build();
        return new QueryExecutionException(message, cause);
    }
    //endregion

    //region DataCollectException initialization
    public DataCollectException dataCollectionFailed(Throwable cause) {
        var message = new MessageBuilder()
                .withContent("Collecting of parameters failed")
                .withCause(cause)
                .build();
        return new DataCollectException(message, cause);
    }

    public DataCollectException dataCollectorNotInitialized(Throwable cause) {
        var message = new MessageBuilder()
                .withContent("Data collector can not be initialized")
                .withCause(cause)
                .build();
        return new DataCollectException(message, cause);
    }
    //endregion

    //region ParseExceptions initialization
    public ParseException parseInputQueryFailed(String query, Throwable cause) {
        var message = new MessageBuilder()
                .withContent("Parsing of input query '%s' failed", query)
                .withCause(cause)
                .build();
        return new ParseException(message, cause);
    }

    public ParseException parseExplainPlanFailed(Throwable cause) {
        var message = new MessageBuilder()
                .withContent("Parsing of explain plan failed")
                .withCause(cause)
                .build();
        return new ParseException(message, cause);
    }

    public ParseException cacheResultFailed(Throwable cause) {
        var message = new MessageBuilder()
                .withContent("Caching of query result failed")
                .withCause(cause)
                .build();
        return new ParseException(message, cause);
    }

    public ParseException consumeResultFailed(Throwable cause) {
        var message = new MessageBuilder()
                .withContent("Consuming of query result failed")
                .withCause(cause)
                .build();
        return new ParseException(message, cause);
    }
    //endregion

    //region WrapperExceptions initialization
    public WrapperException wrapperInitializationFailed(Throwable cause) {
        var message = new MessageBuilder()
                .withContent("Wrapper can't be initialized")
                .withCause(cause)
                .build();
        return new WrapperException(message, cause);
    }
    //endregion

    //region WrapperUnsupportedOperationException initialization
    public WrapperUnsupportedOperationException unsupportedOperation(String operation) {
        var message = new MessageBuilder()
                .withContent("Operation '%s' is not supported", operation)
                .build();
        return new WrapperUnsupportedOperationException(message);
    }
    //endregion

    protected static class MessageBuilder {
        private final StringBuilder _message;

        public MessageBuilder() {
            _message = new StringBuilder();
        }

        public MessageBuilder withContent(String content, Object... args) {
            _message.append(String.format(content, args));
            return this;
        }

        public MessageBuilder withCause(Throwable cause) {
            if (cause != null)
                _message.append(", cause { ").append(cause.getMessage()).append(" }");
            return this;
        }

        public String build() {
            return _message.toString();
        }
    }
}
