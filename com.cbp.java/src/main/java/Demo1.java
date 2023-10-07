import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Author changbp
 * @Date 2022-11-03 22:03
 * @Return
 * @Version 1.0
 */
public class Demo1 {
    public static void main(String[] args) {
        SimpleDateFormat format = new SimpleDateFormat("HH");
        final Date date = new Date();
        System.out.println(format.format(date));
    }

}
