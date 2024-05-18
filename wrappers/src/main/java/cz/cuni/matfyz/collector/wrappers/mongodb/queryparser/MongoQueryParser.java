package cz.cuni.matfyz.collector.wrappers.mongodb.queryparser;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.cuni.matfyz.collector.wrappers.exceptions.ParseException;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


/**
 * Main class handling all process of parsing mongo query to correct mongo command
 */
public abstract class MongoQueryParser {

    /**
     * Method which will split query into tokens for easier parsing
     * @param query inputted query
     * @return instance of parsed tokens
     * @throws ParseException when some problem occur during parsing process
     */
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

    /**
     * Rrivate void for printing tokens to console. Used for debugging purposes.
     * @param tokens tokens to be print
     */
    private static void _printTokens(QueryTokens tokens) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            var stringTokens = mapper.writeValueAsString(tokens);
            System.out.println(stringTokens);

        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

    }

    /**
     * Main function which parse query to command
     * @param query query to be parsed
     * @return parsed command
     * @throws ParseException when some ParseException occur during parsing process
     */
    public static Document parseQueryToCommmand(String query) throws ParseException {
        QueryTokens tokens = _splitToTokens(query);
        CommandBuilder commandBuilder = new CommandBuilder(tokens.collectionName);

        while(tokens.moveNext()) {
            commandBuilder.updateWithFunction(tokens.getActualFunction());
        }
        return commandBuilder.build();
    }
}
