package utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/*
将具体年份转换为十年制的年代（如：2013->2010s, 8->0s, -11->-10s）
 */
public class DecadeConverter {
    public static String toDecade(String year){
        /*
        如果是空字符串或者 null，直接返回
         */
        if (year.trim().isEmpty()){
            return year;
        }

        /*
        如果已经是 YYYYs 的形式，直接返回
         */
        if (year.matches("-?\\d+s")){
            return year;
        }

        /*
        提取数字部分（去掉所有非数字字符，保留负号）
         */
        String numericPart = year.replaceAll("[^0-9.-]", "");
        if (numericPart.isEmpty()){
            return year;
        }

        try {
            float floatYear = Float.parseFloat(numericPart);
            int numericYear = (int) Math.floor(floatYear);
            /*
            计算十年制的起始年份
             */
            int numericDecade = (numericYear / 10) * 10;
            return (numericYear < 0 ? "-" : "") + Math.abs(numericDecade) + "s";
        } catch (Exception e) {
            return year;
        }
    }

    public static int toInt(String decade){
        // 正则匹配：可选负号 + 数字 + 's'
        Pattern pattern = Pattern.compile("^(-?\\d+)s$");
        Matcher matcher = pattern.matcher(decade.trim());

        if (!matcher.find()) {
            throw new IllegalArgumentException("Invalid decade format: " + decade);
        }

        String numericPart = matcher.group(1);

        // 特殊处理 "-0s"
        if ("-0".equals(numericPart)) {
            return 0;
        }

        // 转换为整数
        return Integer.parseInt(numericPart);
    }

    public static void main(String[] args) {
        // String[] testCases = {"1923.0", "-1785.0", "8", "-8", "1850s", "abc", "--1234", "c. 1500"};
        // for (String test : testCases) {
        //     System.out.println(test + " → " + toDecade(test));
        // }
        String[] testCases = {"1920s", "-1785s", "0s", "-0s", "1850s"};
        for (String testCase : testCases) {
            System.out.println(toInt(testCase));
        }

    }
}
