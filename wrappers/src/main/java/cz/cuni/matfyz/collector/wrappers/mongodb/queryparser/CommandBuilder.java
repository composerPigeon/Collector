package cz.cuni.matfyz.collector.wrappers.mongodb.queryparser;

import cz.cuni.matfyz.collector.wrappers.exceptions.ParseException;
import org.bson.Document;

import javax.print.Doc;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

public class CommandBuilder {

    private enum ReturnType {
        Collection,
        Cursor,
        None
    }

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

    private Document _command;
    private ReturnType _returnType;
    private final String _collectionName;

    public CommandBuilder(String collectionName) {
        _collectionName = collectionName;
        _command = new Document();
        _returnType = ReturnType.Collection;
    }

    public Document build() {
        return _command;
    }

    private static final Set<String> FIND_NOTSUPPORTED_OPTIONS = Set.of("explain", "maxAwaitTimeMS", "readPreference");

    private void _updateWithOptions(String functionName, Document options) throws ParseException {
        for (var entry : options.entrySet()) {

            if ("find".equals(functionName) && FIND_NOTSUPPORTED_OPTIONS.contains(entry.getKey()))
                throw new ParseException(invalidOptionErrorMessage("explain", "find"));
            else if ("aggregate".equals(functionName) && "explain".equals(entry.getKey()))
                throw new ParseException(invalidOptionErrorMessage("explain", "aggregate"));
            _command.put(entry.getKey(), entry.getValue());
        }
    }

    private void _updateWithDocumentValue(String keyName, Document documentValue) {
        if (!documentValue.isEmpty())
            _command.put(keyName, documentValue);
    }

    private void _updateWithDocumentArrayValue(String keyName, List<Document> arrayDocValue) {
        if (!arrayDocValue.isEmpty())
            _command.put(keyName, arrayDocValue);
    }



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

    private void _updateWithCollectionAggregate(FunctionItem function) throws ParseException {
        _command.put("aggregate", _collectionName);
        _command.put("cursor", new Document());
        switch (function.args.size()) {
            case 0:
                break;
            case 1:
                _updateWithDocumentArrayValue("pipeline", function.args.getDocumentList(0));
                break;
            case 2:
                _updateWithDocumentArrayValue("pipeline", function.args.getDocumentList(0));
                _updateWithOptions(function.name, function.args.getDocument(1));
                break;
            default:
                throw new ParseException(invalidNumberOfArgumentsInCollectionFunction(function.name));
        }
        _returnType = ReturnType.Cursor;
    }

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

    // Results for count, distinct and aggregate methods heave different format and also will require more specific explain tree parsing
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

    private void _updateWithCursorFlagMethod(FunctionItem function) throws ParseException {
        if (function.args.size() == 0)
            _command.put(function.name, true);
        else if (function.args.size() == 1)
            _command.put(function.name, function.args.getBoolean(0));
        else
            throw new ParseException(invalidNumberOfArgumentsInCursorFunction(function.name));
        _returnType = ReturnType.Cursor;
    }

    private void _updateWithCursorIntegerMethod(FunctionItem function) throws ParseException {
        if (function.args.size() == 1)
            _command.put(function.name, function.args.getInteger(0));
        else
            throw new ParseException(invalidNumberOfArgumentsInCursorFunction(function.name));
        _returnType = ReturnType.Cursor;
    }

    private void _updateWithCursorDocumentMethod(FunctionItem function) throws ParseException {
        if (function.args.size() == 1)
            _command.put(function.name, function.args.getDocument(0));
        else
            throw new ParseException(invalidNumberOfArgumentsInCursorFunction(function.name));
        _returnType = ReturnType.Cursor;
    }

    private void _updateWithCursorStringMethod(FunctionItem function) throws ParseException {
        if (function.args.size() == 1)
            _command.put(function.name, function.args.getString(0));
        else
            throw new ParseException(invalidNumberOfArgumentsInCursorFunction(function.name));
        _returnType = ReturnType.Cursor;
    }

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
