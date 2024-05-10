package cz.cuni.matfyz.collector.wrappers.mongodb.queryparser;

import cz.cuni.matfyz.collector.wrappers.exceptions.ParseException;

import java.util.ArrayList;
import java.util.List;

public class QueryTokens {
    public final String db;
    public final String collectionName;
    public final FunctionItem[] functionTokens;

    private int _index;

    private QueryTokens(String db, String collectionName, FunctionItem[] functionTokens) {
        this.db = db;
        this.collectionName = collectionName;
        this.functionTokens = functionTokens;
        _index = -1;
    }

    public boolean moveNext() {
        _index += 1;
        return _index < functionTokens.length;
    }

    public FunctionItem getActualFunction() {
        return functionTokens[_index];
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();

        buffer.append("QueryTokens:\n");

        buffer.append("- DB: ").append(db).append('\n');
        buffer.append("- Collection: ").append(collectionName).append('\n');
        buffer.append("- Functions:\n");

        for (var function : functionTokens) {
            buffer.append("  - name: ").append(function.name).append('\n');
            buffer.append("    - args: ").append(function.args.toString());
        }
        return buffer.toString();
    }

    public static class Builder {
        private String _db;
        private String _collectionName;
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
            return new QueryTokens(
                    _db,
                    _collectionName,
                    _functionTokens.toArray(FunctionItem[]::new)
            );
        }
    }
}
