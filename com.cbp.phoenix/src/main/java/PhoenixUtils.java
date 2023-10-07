import java.io.IOException;
import java.util.Properties;

/**
 * @Author changbp
 * @Date 2021-04-25 16:04
 * @Return
 * @Version 1.0
 */
public class PhoenixUtils {
    public static Properties pro = new Properties();

    static {
        try {
            pro.load(ClassLoader.getSystemResourceAsStream("phoenix.properties"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static String getDriver() throws IOException {
        pro.load(ClassLoader.getSystemResourceAsStream("phoenix.properties"));
        return pro.getProperty("phoenix.driver");
    }

    public static String getUrl() {
        return pro.getProperty("phoenix.url");
    }

    public static String getUserName() {
        return pro.getProperty("phoenix.user");
    }

    public static String getPassWord() {
        return pro.getProperty("phoenix.password");
    }

    public static void main(String[] args) throws IOException {
        System.out.println(getDriver());
        System.out.println(getUrl());
        System.out.println(getUserName());
        System.out.println(getPassWord());
    }
}

