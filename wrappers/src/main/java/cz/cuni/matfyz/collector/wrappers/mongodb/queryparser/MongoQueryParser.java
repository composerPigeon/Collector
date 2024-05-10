package cz.cuni.matfyz.collector.wrappers.mongodb.queryparser;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.cuni.matfyz.collector.wrappers.exceptions.ParseException;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class MongoQueryParser {
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

    private static void _printTokens(QueryTokens tokens) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            var stringTokens = mapper.writeValueAsString(tokens);
            System.out.println(stringTokens);

        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

    }

    public static Document parseQueryToCommmand(String query) throws ParseException {
        QueryTokens tokens = _splitToTokens(query);
        System.out.println(tokens.toString());
        CommandBuilder commandBuilder = new CommandBuilder(tokens.collectionName);

        while(tokens.moveNext()) {
            commandBuilder.updateWithFunction(tokens.getActualFunction());
        }
        return commandBuilder.build();
    }
}
