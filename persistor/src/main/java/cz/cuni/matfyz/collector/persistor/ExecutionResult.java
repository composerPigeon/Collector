package cz.cuni.matfyz.collector.persistor;


import java.util.Map;

public class ExecutionResult {
    private final boolean _isSuccessful;
    private final Map<String, Object> _result;
    private final String _errorMessage;

    private ExecutionResult(Map<String, Object> result) {
        _isSuccessful = true;
        _result = result;
        _errorMessage = null;
    }

    public static ExecutionResult success(Map<String, Object> result) {
        return new ExecutionResult(result);
    }

    public static ExecutionResult error(String errorMessage) {
        return new ExecutionResult(errorMessage);
    }

    private ExecutionResult(String errorMessage) {
        _isSuccessful = false;
        _result = null;
        _errorMessage = errorMessage;
    }

    public boolean isSuccessful() {
        return _isSuccessful;
    }

    public Map<String, Object> getResult() {
        return _result;
    }

    public String getErrorMessage() {
        return _errorMessage;
    }
}
