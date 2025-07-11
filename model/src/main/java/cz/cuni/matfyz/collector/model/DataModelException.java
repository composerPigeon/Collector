package cz.cuni.matfyz.collector.model;

public class DataModelException extends Exception {
    public DataModelException(String message) { super(message);}
    public DataModelException(String message, Throwable cause) {
        super(DataModelException.createMessageWithCause(message, cause), cause);
    }

    private static String createMessageWithCause(String message, Throwable cause) {
        String causeMessage = cause.getMessage();

        while (cause.getCause() != null) {
            cause = cause.getCause();
            causeMessage = cause.getMessage();
        }

        return String.format(message + "{ %s }", causeMessage);
    }
}
