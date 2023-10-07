package com.cbp.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;
import java.util.Properties;
import java.util.ResourceBundle;

/**
 * @Author changbp
 * @Date 2021-06-04 17:56
 * @Return
 * @Version 1.0
 */


/**
 * @Author changbp
 * @Date 2021-06-04 14:31
 * @Return
 * @Version 1.0
 */
public class ReadProperties {
    public static void main(String[] args) {
//        final ResourceBundle config = getConfig("config");
//        final String url = config.getString("url");
//        final String user = config.getString("user");
//        final String passwd = config.getString("passwd");
//        final Properties config1 = getConfig("config.properties", ReadProperties.class.getClassLoader());
        final Properties config1 = getConfig();
        final String url = config1.getProperty("url");
        final String user = config1.getProperty("user");
        final String passwd = config1.getProperty("passwd");
        System.out.println(url);
        System.out.println(user);
        System.out.println(passwd);

    }


    /*
     * ResourceBundle 方式读取类路劲下的properties文件，需要指定文件名不需要后缀
     * */
    public static ResourceBundle getConfig(String propertiesName) {
        return ResourceBundle.getBundle(propertiesName, Locale.ENGLISH);
    }

    /*
     * 使用classloader 获取类路径下的properties文件，需要指定文件名需要后缀
     * */
    public static Properties getConfig(String propertiesName, ClassLoader classLoader) {
        final Properties properties = new Properties();
        final InputStream resourceAsStream = classLoader.getResourceAsStream(propertiesName);
        try {
            properties.load(resourceAsStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }

    /*
     * 使用字节流读取文件，优点任意位置的文件
     * */
    public static Properties getConfig(){
        final Properties properties = new Properties();
        try {
            final BufferedReader bufferedReader = new BufferedReader(new FileReader("D:\\MySpace\\Studay\\com.cbp.mysql\\src\\main\\resources\\config.properties"));
            properties.load(bufferedReader);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }
}
