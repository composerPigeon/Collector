package cz.cuni.matfyz.collector.wrappers.exceptions;

public class WrapperUnsupportedOperationException extends WrapperException {

    public WrapperUnsupportedOperationException(String message) {
        super(message);
    }

    public WrapperUnsupportedOperationException(Throwable cause) {
        super(cause);
    }

    public WrapperUnsupportedOperationException(String message, Throwable cause) {
        super(message, cause);
    }
}
