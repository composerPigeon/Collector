package cz.cuni.matfyz.collector.wrappers.mongodb.queryparser;

import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class ArgumentsArray {
    private String[] _array;

    private ArgumentsArray(String[] array) {
        _array = array;
    }

    public String getString(int index) {
        return _array[index];
    }
    public Document getDocument(int index) {
        return Document.parse(_array[index]);
    }
    public int getInteger(int index) {
        return Integer.parseInt(_array[index]);
    }
    public double getDouble(int index) {
        return Double.parseDouble(_array[index]);
    }

    public int size() {
        return _array.length;
    }

    private enum ArgParseType {
        InObject,
        InArray,
        InString,
        InDoubleString,
        InNumber,
        InBoolean,
        Out
    }

    private static class ArgParseState {
        public ArgParseType type;
        public int indentation;
        public char prevChar;

        public ArgParseState() {
            type = ArgParseType.Out;
            indentation = 0;
            prevChar = '\0';
        }
    }

    public static ArgumentsArray parseArguments(String argContent) {
        if (argContent == null)
            return new ArgumentsArray(new String[0]);

        String content = argContent.trim();
        ArgParseState state = new ArgParseState();

        StringBuilder buffer = new StringBuilder();
        List<String> args = new ArrayList<>();

        for (char ch : content.toCharArray()) {
            if (state.type == ArgParseType.Out) {
                if (ch == '"')
                    state.type = ArgParseType.InDoubleString;
                if (ch == '\'')
                    state.type = ArgParseType.InString;
                else if (ch == '{') {
                    state.type = ArgParseType.InObject;
                    buffer.append(ch);
                } else if (ch == '[') {
                    state.type = ArgParseType.InArray;
                    buffer.append(ch);
                } else if (ch == ',') {
                    args.add(buffer.toString());
                    buffer.setLength(0);
                } else if (Character.isDigit(ch)) {
                    state.type = ArgParseType.InNumber;
                    buffer.append(ch);
                } else if (!Character.isWhitespace(ch)) {
                    state.type = ArgParseType.InBoolean;
                    buffer.append(ch);
                }

            } else if (state.type == ArgParseType.InObject) {
                if (ch == '{')
                    state.indentation += 1;
                else if (ch == '}' && state.indentation == 0) {
                    state.type = ArgParseType.Out;
                } else if (ch == '}')
                    state.indentation -= 1;
                buffer.append(ch);
            } else if (state.type == ArgParseType.InArray) {
                if (ch == '[')
                    state.indentation += 1;
                else if (ch == ']' && state.indentation == 0) {
                    state.type = ArgParseType.Out;
                } else if (ch == ']')
                        state.indentation -= 1;
                buffer.append(ch);
            } else if (state.type == ArgParseType.InString) {
                if (ch == '\'' && state.prevChar != '\\')
                    state.type = ArgParseType.Out;
                else {
                    buffer.append(ch);
                }
            } else if (state.type == ArgParseType.InDoubleString) {
                if (ch == '"' && state.prevChar != '\\')
                    state.type = ArgParseType.Out;
                else {
                    buffer.append(ch);
                }
            } else if (state.type == ArgParseType.InNumber) {
                if (Character.isWhitespace(ch)) {
                    state.type = ArgParseType.Out;
                } else
                    buffer.append(ch);
            } else if (state.type == ArgParseType.InBoolean) {
                if (Character.isWhitespace(ch)) {
                    state.type = ArgParseType.Out;
                } else {
                    buffer.append(ch);
                }
            }
            state.prevChar = ch;
        }

        if (!buffer.isEmpty()) {
            args.add(buffer.toString());
        }

        return new ArgumentsArray(args.toArray(String[]::new));
    }
 }
