package cz.cuni.matfyz.collector.wrappers.abstractwrapper;

public class ExplainParseException extends WrapperException{
    public ExplainParseException(String message) { super(message); }
    public ExplainParseException(Throwable cause) { super(cause); }
    public ExplainParseException(String message, Throwable cause) { super(message, cause); }
}
