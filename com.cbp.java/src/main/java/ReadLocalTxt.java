import java.io.BufferedReader;
import java.io.FileReader;

/**
 * @Author changbp
 * @Date 2021/1/25 17:16
 * @Version 1.0
 */
//读取文本文件
public class ReadLocalTxt {
    public static void main(String[] args) {
        StringBuilder b = new StringBuilder();
        int count = 0;
        try {
            BufferedReader br = new BufferedReader(new FileReader("D:\\test.txt"));//构造一个BufferedReader类来读取文件
            String s = null;

            while ((s = br.readLine()) != null) {//使用readLine方法，一次读一行
                b.append(s);
                b.append(",");
                count++;
                if (count > 200) {
                    break;
                }
            }

            br.close();
            System.out.println(count);
            System.out.println(b.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
