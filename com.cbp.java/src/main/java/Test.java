import java.sql.Array;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.IntStream;

/**
 * @Author changbp
 * @Date 2022-12-07 15:29
 * @Return
 * @Version 1.0
 */
public class Test {
    public static void main(String[] args) {
//        int[] nums = {-100, 4, -80, 6, -7, 5};
//        System.out.println(maximumProduct(nums));
//        String name_is = reverseWords("name is");
//        System.out.println(name_is);

        String d = "2023-12-03 00:00:00";
        String d1 = "2023-12-09 00:00:00";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            Date date = simpleDateFormat.parse(d);
            Date date2 = simpleDateFormat.parse(d1);
            long time = date.getTime();
            long time2 = date2.getTime();
            System.out.println(time);
            System.out.println(time2);
        } catch (ParseException e) {
            e.printStackTrace();
        }


    }

    /*
     * 求数组中最大的三个数之积（）
     * */
    public static int maximumProduct(int[] nums) {
        Arrays.sort(nums);
        return Math.max(nums[nums.length - 1] * nums[nums.length - 2] * nums[nums.length - 3],
                nums[0] * nums[1] * nums[nums.length - 1]);
    }

    /*
     *  反转字符创中的每个单词，单词之间空格间隔
     * */
    public static String reverseWords(String s) {
        String[] strs = s.split(" ");
        StringBuffer newStr = new StringBuffer();
        for (String str : strs) {
            StringBuffer newStr1 = new StringBuffer();
            for (int i = str.length() - 1; i >= 0; i--) {
                char c = str.charAt(i);
                newStr1.append(Character.toString(c));
            }
            newStr.append(newStr1 + " ");
        }
        return newStr.toString().trim();
    }

    /*
     * 判断字符串中字符是否唯一
     * */
    public boolean isUnique(String astr) {
        HashSet<Character> seen = new HashSet<>();
        for (char c : astr.toCharArray()) {
            if (seen.contains(c)) {
                return false;
            } else {
                seen.add(c);
            }
        }
        return true;
    }

    /*
     * 给定两个由小写字母组成的字符串 s1 和 s2，请编写一个程序，确定其中一个字符串的字符重新排列后，能否变成另一个字符串。
     * */
    public boolean CheckPermutation(String s1, String s2) {
        if (s1.length() != s2.length()) {
            return false;
        }
        char[] c1 = s1.toCharArray();
        char[] c2 = s2.toCharArray();
        Arrays.sort(c1);
        Arrays.sort(c2);
        for (int i = 0; i < c1.length; i++) {
            if (c1[i] != c2[i]) {
                return false;
            }
        }
        return true;
    }
}
