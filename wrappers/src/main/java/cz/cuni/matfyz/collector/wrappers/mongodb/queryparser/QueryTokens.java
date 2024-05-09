package cz.cuni.matfyz.collector.wrappers.mongodb.queryparser;

import cz.cuni.matfyz.collector.wrappers.exceptions.ParseException;

import java.util.ArrayList;
import java.util.List;

public class QueryTokens {
    public final String db;
    public final String collectionName;
    public final FunctionItem[] functionTokens;

    private QueryTokens(String db, String collectionName, FunctionItem[] functionTokens) {
        this.db = db;
        this.collectionName = collectionName;
        this.functionTokens = functionTokens;
    }

    public static class Builder {
        private String _db;
        private String _collectionName;
        private FunctionItem _findToken;
        private final List<FunctionItem> _functionTokens;



        private static FunctionItem _parseToFunctionItem(String token) {
            StringBuilder buffer = new StringBuilder();
            boolean inArgs = false;

            String name = null;
            String content = null;

            for (char ch : token.toCharArray()) {
                if (inArgs) {
                    if (ch == ')') {
                        content = buffer.toString().trim();
                        break;
                    } else {
                        buffer.append(ch);
                    }

                } else {
                    if (ch == '(') {
                        inArgs = true;
                        name = buffer.toString();
                        buffer.setLength(0);
                    } else {
                        buffer.append(ch);
                    }
                }
            }

            return new FunctionItem(name, ArgumentsArray.parseArguments(content));
        }

        public Builder() {
            _functionTokens = new ArrayList<>();
        }

        public void addToken(String token) {
            if (_db == null)
                _db = token;
            else if (_collectionName == null) {
                if (token.startsWith("getCollection(")) {
                    var item = _parseToFunctionItem(token);
                    _collectionName = item.args.getString(0);
                } else {
                    _collectionName = token;
                }
            } else {
                _functionTokens.add(_parseToFunctionItem(token));
            }
        }

        public QueryTokens toTokens() throws ParseException {
            if (_findToken == null)
                throw new ParseException("Cannot parse query with less then 3 clauses");
            else if (!"find".equals(_findToken.name)) {
                throw new ParseException("Cannot parse non find query");
            }
            return new QueryTokens(
                    _db,
                    _collectionName,
                    _functionTokens.toArray(FunctionItem[]::new)
            );
        }
    }
}
