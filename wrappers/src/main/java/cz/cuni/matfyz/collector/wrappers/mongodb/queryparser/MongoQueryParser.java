package cz.cuni.matfyz.collector.wrappers.mongodb.queryparser;

import cz.cuni.matfyz.collector.wrappers.exceptions.ParseException;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class MongoQueryParser {

    private static final Set<String> FUNC_AGG = Set.of("count", "aggregation", "");
    private static QueryTokens _splitToTokens(String query) throws ParseException {
        StringBuilder buffer = new StringBuilder();
        QueryTokens.Builder tokensBuilder = new QueryTokens.Builder();

        boolean isInsideArgs = false;

        for (char ch : query.toCharArray()) {
            if (isInsideArgs) {
                if (ch == ')')
                    isInsideArgs = false;
                buffer.append(ch);
            } else {
                if (ch == '.') {
                    tokensBuilder.addToken(buffer.toString());
                    buffer.setLength(0);
                } else {
                    if (ch == '(')
                        isInsideArgs = true;
                    buffer.append(ch);
                }
            }
        }

        if (!buffer.isEmpty())
            tokensBuilder.addToken(buffer.toString());

        return tokensBuilder.toTokens();
    }

    private static Document _updateCommandWithAggFunction(FunctionItem functionItem, Document command)  {
        var newCommand = new Document();
        newCommand.put(functionItem.name, 1);
        newCommand.put("query", command);
        return newCommand;
    }

    private static Document _updateCommandWithFunctionItem(FunctionItem function, Document command) throws ParseException {
        if (FUNC_AGG.contains(function.name)) {
            return _updateCommandWithAggFunction(function, command);
        }

        if ("limit".equals(function.name)) {
            command.put("limit", function.args.getInteger(0));
            return command;
        } else if ("project".equals(function.name)) {
            command.put("projection", function.args.getDocument(0));
            return command;
        } else if ("skip".equals(function.name)) {
            command.put("skip", function.args.getInteger(0));
            return command;
        } else if ("count".equals(function.name)) {
            var newCommand = new Document();
            newCommand.put("count", 1);
            newCommand.put("query", command);
            return newCommand;
        } else {
            throw new ParseException("Function name: " + function.name + " cannot be parsed from query.");
        }
    }

    public static Document parseQueryToCommmand(String query) throws ParseException {
        Document command = new Document();
        QueryTokens tokens = _splitToTokens(query);

        if ("find".equals(tokens.findToken.name)) {
            command.put(tokens.findToken.name, tokens.collectionName);
            if (tokens.findToken.args.size() == 1)
                command.put("filter", tokens.findToken.args.getDocument(0));
            else if (tokens.findToken.args.size() == 2) {
                command.put("filter", tokens.findToken.args.getDocument(0));
                command.put("projection", tokens.findToken.args.getDocument(1));
            }
            //TODO: more cases

            for (FunctionItem function : tokens.functionTokens) {
                command = _updateCommandWithFunctionItem(function, command);
            }

            return command;
        } else {
            throw new ParseException("Query: " + query + " does not contain find clause");
        }
    }
}
