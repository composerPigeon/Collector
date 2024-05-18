package cz.cuni.matfyz.collector.wrappers.mongodb.queryparser;

import cz.cuni.matfyz.collector.wrappers.exceptions.ParseException;
import org.bson.Document;

import javax.print.Doc;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * Class which is responsible for building all of the supported commands from mongosh functions
 */
public class CommandBuilder {

    /**
     * Enum which represents actual return type of last called function, that was used to update command
     */
    private enum ReturnType {
        Collection,
        Cursor,
        None
    }

    //Methods for getting error messages
    private static String invalidNumberOfArgumentsInCollectionFunction(String functionName) {
        return "Invalid number of arguments in collection method " + functionName;
    }
    private static String invalidNumberOfArgumentsInCursorFunction(String functionName) {
        return "Invalid number of arguments in cursor method " + functionName;
    }
    private static String nonExistentFunctionErrorMessage(String functionName) {
        return "Function " + functionName + " does not exist or is not supported.";
    }
    private static String invalidOptionErrorMessage(String optionName, String functionName) {
        return "Option " + optionName + " is not supported in method " + functionName + ".";
    }
    private static final String INVALID_COUNT_USAGE_MSG = "Count cursor method is supported only on Cursor returned from find() function.";

    /**
     * Field which stores actual command, that is incrementally built during while process
     */
    private Document _command;

    /** Field which stores last returnType */
    private ReturnType _returnType;

    /** Field which stores on which mongodb document collection is this command built */
    private final String _collectionName;

    public CommandBuilder(String collectionName) {
        _collectionName = collectionName;
        _command = new Document();
        _returnType = ReturnType.Collection;
    }

    /**
     * Builder method for building final database command as Document
     * @return the final command as Document
     */
    public Document build() {
        return _command;
    }

    /**
     * Private static field which contains name of options, which are not supported to use with our find command
     */
    private static final Set<String> FIND_NOTSUPPORTED_OPTIONS = Set.of("explain", "maxAwaitTimeMS", "readPreference");

    /**
     * Method which will update actual command with options object represented as document
     * @param functionName name of function by which is command updated
     * @param options instance of Document which represents the inputted options
     * @throws ParseException when invalid option is used in options object which we do not support
     */
    private void _updateWithOptions(String functionName, Document options) throws ParseException {
        for (var entry : options.entrySet()) {

            if ("find".equals(functionName) && FIND_NOTSUPPORTED_OPTIONS.contains(entry.getKey()))
                throw new ParseException(invalidOptionErrorMessage("explain", "find"));
            else if ("aggregate".equals(functionName) && "explain".equals(entry.getKey()))
                throw new ParseException(invalidOptionErrorMessage("explain", "aggregate"));
            _command.put(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Method which will update actual command by some document value parsed from arguments of functions
     * @param keyName name which will be used as a key name in document command
     * @param documentValue instance of document to be set as value
     */
    private void _updateWithDocumentValue(String keyName, Document documentValue) {
        if (!documentValue.isEmpty())
            _command.put(keyName, documentValue);
    }

    /**
     * Method which will update actual command by some document list value
     * @param keyName name which will be used as a key name in document command
     * @param arrayDocValue insatnce of list to be used as newly added value
     */
    private void _updateWithDocumentListValue(String keyName, List<Document> arrayDocValue) {
        if (!arrayDocValue.isEmpty())
            _command.put(keyName, arrayDocValue);
    }

    /**
     * Method called on Collection type for updating actual command as a count.
     * Is not used, because after some testing I figured out that result and explain structure is too different from find command.
     * @param function inputted function to update command
     * @throws ParseException when function is called with incorrect count of parameters or some values cannot be parsed
     */
    private void _updateWithCollectionCount(FunctionItem function) throws ParseException {
        _command.put("count", _collectionName);
        switch (function.args.size()) {
            case 0:
                break;
            case 1:
                _updateWithDocumentValue("query", function.args.getDocument(0));
                break;
            case 2:
                _updateWithDocumentValue("query", function.args.getDocument(0));
                _updateWithOptions(function.name, function.args.getDocument(1));
                break;
            default:
                throw new ParseException(invalidNumberOfArgumentsInCollectionFunction(function.name));
        }
    }

    /**
     * Method called on Collection type for updating actual command as an aggregate.
     * Is not used, because after some testing I figured out that result and explain structure is too different from find command.
     * @param function inputted function to update command
     * @throws ParseException when function is called with incorrect count of parameters or some values cannot be parsed
     */
    private void _updateWithCollectionAggregate(FunctionItem function) throws ParseException {
        _command.put("aggregate", _collectionName);
        _command.put("cursor", new Document());
        switch (function.args.size()) {
            case 0:
                break;
            case 1:
                _updateWithDocumentListValue("pipeline", function.args.getDocumentList(0));
                break;
            case 2:
                _updateWithDocumentListValue("pipeline", function.args.getDocumentList(0));
                _updateWithOptions(function.name, function.args.getDocument(1));
                break;
            default:
                throw new ParseException(invalidNumberOfArgumentsInCollectionFunction(function.name));
        }
        _returnType = ReturnType.Cursor;
    }

    /**
     * Method for updating actual command as a find command.
     * @param function inputted function to update command
     * @throws ParseException when function is called with incorrect count of parameters or some values cannot be parsed
     */
    private void _updateWithCollectionFind(FunctionItem function) throws ParseException {
        _command.put("find", _collectionName);
        switch (function.args.size()) {
            case 0:
                break;
            case 1:
                _updateWithDocumentValue("filter", function.args.getDocument(0));
                break;
            case 2:
                _updateWithDocumentValue("filter", function.args.getDocument(0));
                _updateWithDocumentValue("projection", function.args.getDocument(1));
                break;
            case 3:
                _updateWithDocumentValue("filter", function.args.getDocument(0));
                _updateWithDocumentValue("projection", function.args.getDocument(1));
                _updateWithOptions(function.name, function.args.getDocument(2));
                break;
            default:
                throw new ParseException(invalidNumberOfArgumentsInCollectionFunction(function.name));
        }
        _returnType = ReturnType.Cursor;
    }

    /**
     * Method called on Collection type for updating actual command as a distinct.
     * Is not used, because after some testing I figured out that result and explain structure is too different from find command.
     * @param function inputted function to update command
     * @throws ParseException when function is called with incorrect count of parameters or some values cannot be parsed
     */
    private void _updateWithCollectionDistinct(FunctionItem function) throws ParseException {
        _command.put("distinct", _collectionName);

        switch (function.args.size()) {
            case 0:
                break;
            case 1:
                _command.put("key", function.args.getString(0));
                break;
            case 2:
                _command.put("key", function.args.getString(0));
                _updateWithDocumentValue("query", function.args.getDocument(1));
                break;
            case 3:
                _command.put("key", function.args.getString(0));
                _updateWithDocumentValue("query", function.args.getDocument(1));
                _updateWithOptions(function.name, function.args.getDocument(2));
                break;
            default:
                throw new ParseException(invalidNumberOfArgumentsInCollectionFunction(function.name));
        }

        _returnType = ReturnType.None;
    }

    /**
     * Method which takes a collection function to update command
     * and decide which of the supported functions to use based on functions name
     * @param function inputted function
     * @throws ParseException when function of nonexistent name or function our system do not support was used
     */
    private void _updateWithCollectionFunction(FunctionItem function) throws ParseException {
        if ("find".equals(function.name))
            _updateWithCollectionFind(function);
        //else if ("aggregate".equals(function.name))
        //    _updateWithCollectionAggregate(function);
        //else if ("count".equals(function.name))
        //    _updateWithCollectionCount(function);
        //else if ("distinct".equals(function.name))
        //    _updateWithCollectionDistinct(function);
        else
            throw new ParseException(nonExistentFunctionErrorMessage(function.name));
    }

    /**
     * Public method which is used to update our command by some function
     * @param function function with which we want to modify our command
     * @throws ParseException throws when function does not exist or cannot be called on lastly returned type or some problems with arguments occur
     */
    public void updateWithFunction(FunctionItem function) throws ParseException {
        switch (_returnType) {
            case Collection:
                _updateWithCollectionFunction(function);
                break;
            case Cursor:
                _updateWithCursorFunction(function);
                break;
            case None:
                throw new ParseException("You are calling function on unsupported type. You can call specific functions on cursor or collection.");
        }
    }

    /**
     * Method to update our command with some flag function that was called on cursor
     * @param function inputted function
     * @throws ParseException when function has invalid number of arguments
     */
    private void _updateWithCursorFlagMethod(FunctionItem function) throws ParseException {
        if (function.args.size() == 0)
            _command.put(function.name, true);
        else if (function.args.size() == 1)
            _command.put(function.name, function.args.getBoolean(0));
        else
            throw new ParseException(invalidNumberOfArgumentsInCursorFunction(function.name));
        _returnType = ReturnType.Cursor;
    }

    /**
     * Method for updating command by function which argument should be integer
     * @param function inputted function
     * @throws ParseException when function is called with invalid number of arguments, or some of the arguments couldn't be parsed
     */
    private void _updateWithCursorIntegerMethod(FunctionItem function) throws ParseException {
        if (function.args.size() == 1)
            _command.put(function.name, function.args.getInteger(0));
        else
            throw new ParseException(invalidNumberOfArgumentsInCursorFunction(function.name));
        _returnType = ReturnType.Cursor;
    }

    /**
     * Method for updating command by function which argument should be a document
     * @param function inputted function
     * @throws ParseException when function is called with invalid number of arguments, or some of the arguments couldn't be parsed
     */
    private void _updateWithCursorDocumentMethod(FunctionItem function) throws ParseException {
        if (function.args.size() == 1)
            _command.put(function.name, function.args.getDocument(0));
        else
            throw new ParseException(invalidNumberOfArgumentsInCursorFunction(function.name));
        _returnType = ReturnType.Cursor;
    }

    /**
     * Method for updating command by function which argument should be a string
     * @param function inputted function
     * @throws ParseException when function is called with invalid number of arguments, or some of the arguments couldn't be parsed
     */
    private void _updateWithCursorStringMethod(FunctionItem function) throws ParseException {
        if (function.args.size() == 1)
            _command.put(function.name, function.args.getString(0));
        else
            throw new ParseException(invalidNumberOfArgumentsInCursorFunction(function.name));
        _returnType = ReturnType.Cursor;
    }

    /**
     * Method which updates command by hint function
     * @param function inputted function
     * @throws ParseException when function is called with invalid number of arguments, or some of the arguments couldn't be parsed
     */
    private void _updateWithCursorHint(FunctionItem function) throws ParseException {
        if (function.args.size() == 1) {
            try {
                Document hintDocument = function.args.getDocument(0);
                _command.put("hint", hintDocument);
            } catch (ParseException e) {
                _command.put("hint", function.args.getString(0));
            }
        } else {
            throw new ParseException(invalidNumberOfArgumentsInCursorFunction(function.name));
        }
        _returnType = ReturnType.Cursor;
    }

    /**
     * Method which updates command by count function.
     * It is not used, because count produce different format of result and explain tree
     * @param function inputted function
     * @throws ParseException when function is called with invalid number of arguments, or some of the arguments couldn't be parsed
     */
    private void _updateWithCursorCount(FunctionItem function) throws ParseException {
        if (function.args.size() == 0) {
            if (_command.containsKey("find")) {
                _command.remove("find");
                _command.put("count", _collectionName);
                if (_command.containsKey("filter")) {
                    Document filterDoc = _command.get("filter", Document.class);
                    _command.remove("filter");
                    _command.put("query", filterDoc);
                }
            } else {
                throw new ParseException(INVALID_COUNT_USAGE_MSG);
            }

        } else {
            throw new ParseException(invalidNumberOfArgumentsInCursorFunction(function.name));
        }
        _returnType = ReturnType.None;
    }

    /**
     * Method that decides which types of cursor method is used to update actual command
     * @param function inputted function
     * @throws ParseException when function does not exist or is not supported
     */
    private void _updateWithCursorFunction(FunctionItem function) throws ParseException {
        switch(function.name) {
            case "allowDiskUse", "allowPartialResults", "noCursorTimeout", "returnKey", "showRecordId", "tailable":
                _updateWithCursorFlagMethod(function);
                break;
            case "batchSize", "limit", "maxTimeMS", "skip":
                _updateWithCursorIntegerMethod(function);
                break;
            case "collation", "max", "min", "sort":
                _updateWithCursorDocumentMethod(function);
                break;
            case "comment", "readConcern":
                _updateWithCursorStringMethod(function);
                break;
            case "hint":
                _updateWithCursorHint(function);
                break;
            //case "count":
            //    _updateWithCursorCount(function);
            //    break;
            default:
                throw new ParseException(nonExistentFunctionErrorMessage(function.name));
        }
    }
}
