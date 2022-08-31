package com.maguasen.mybatisplus.hive;

public class HiveUtils {
    public static String replaceLast(String string, String toReplace, String replacement) {
        int pos = string.lastIndexOf(toReplace);
        if (pos > -1) {
            return string.substring(0, pos)
                    + replacement
                    + string.substring(pos + toReplace.length(), string.length());
        } else {
            return string;
        }
    }

    public static void main(String[] args) {
        String s = replaceLast("ab,cas,d,", ",", "");
        System.out.println(s);
    }
}
