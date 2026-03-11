package sqlancer.mysql;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import sqlancer.common.query.ExpectedErrors;

public final class MySQLErrors {

    private MySQLErrors() {
    }

    public static List<String> getExpressionErrors() {
        ArrayList<String> errors = new ArrayList<>();

        errors.add("BIGINT value is out of range"); // e.g., CAST(-('-1e500') AS SIGNED)
        errors.add("is not valid for CHARACTER SET");

        if (MySQLBugs.bug111471) {
            errors.add("Memory capacity exceeded");
        }

        return errors;
    }

    public static List<Pattern> getExpressionRegexErrors() {
        ArrayList<Pattern> errors = new ArrayList<>();

        if (MySQLBugs.bug114533) {
            errors.add(Pattern.compile("For input string: \"0+-0\"")); // match: For input string:
                                                                       // "00000000000000000000-0"
        }

        errors.add(Pattern.compile("Unknown column '.*' in 'order clause'"));

        return errors;
    }

    public static void addExpressionErrors(ExpectedErrors errors) {
        errors.addAll(getExpressionErrors());
        errors.addAllRegexes(getExpressionRegexErrors());
    }

    public static List<String> getInsertUpdateErrors() {
        ArrayList<String> errors = new ArrayList<>();

        errors.add("doesn't have a default value");
        errors.add("Data truncation");
        errors.add("Incorrect integer value");
        errors.add("Duplicate entry");
        errors.add("Data truncated for column");
        errors.add("Data truncated for functional index");
        errors.add("cannot be null");
        errors.add("Incorrect decimal value");
        errors.add("The value specified for generated column");

        errors.add("Incorrect string value");
        errors.add("Invalid utf8mb4 character string");
        errors.add("Cannot convert");
        errors.add("is not valid for CHARACTER SET");
        // 新增：文件系统不支持压缩
        errors.add("Punch hole not supported");
        errors.add("Compression failed");
        // 新增：字符串插入数值列
        errors.add("Incorrect DOUBLE value");
        errors.add("Incorrect FLOAT value");
        errors.add("Incorrect integer value");

        return errors;
    }

    public static void addInsertUpdateErrors(ExpectedErrors errors) {
        errors.addAll(getInsertUpdateErrors());
    }

}
