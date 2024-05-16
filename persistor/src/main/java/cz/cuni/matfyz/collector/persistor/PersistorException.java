package cz.cuni.matfyz.collector.persistor;

public class PersistorException extends Exception {
    public PersistorException(Throwable cause) { super(cause);}
    public PersistorException(String message, Throwable cause) { super(message, cause);}
    public PersistorException(String message) { super(message);}
}
