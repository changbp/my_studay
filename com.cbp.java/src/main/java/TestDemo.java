

import org.apache.commons.codec.binary.Base64;

import javax.crypto.*;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;


/**
 * @Author changbp
 * @Date 2021-07-19 10:30
 * @Return
 * @Version 1.0
 */
public class TestDemo {
    private static final String KEY_ALGORITHM = "AES";
    private static final String DEFAULT_CIPHER_ALGORITHM = "AES/CBC/PKCS5Padding";//默认的加密算法 AES/ECB/PKCS5Padding
    private static final String KEY = "BF4703E6F8D88996AB95B7A9B41D43EE462F11C5";
    //偏移量，AES 128位数据块对应偏移量为16位
    public static final String VIPARA = "1234567890abcdef";   //AES 128位数据块对应偏移量为16位
    public static void main(String[] args){
        String username = "admin";
        String pwd = "123456";

        String userName = TestDemo.encrypt(username, KEY);
        System.out.println("加密后的密码(userName):" + userName);
        String pwD = TestDemo.encrypt(pwd, KEY);
        System.out.println("加密后的密码(pwd):" + pwD);

        String decryptUser = TestDemo.decrypt(userName, KEY);
        String pwD1 = TestDemo.decrypt(pwD, KEY);
        System.out.println(decryptUser);
        System.out.println(pwD1);

    }
    /**
     * AES 加密操作
     *
     * @param content 待加密内容
     * @param key 加密密码
     * @return 返回Base64转码后的加密数据
     */
    public static String encrypt(String content, String key) {
        try {
            Cipher cipher = Cipher.getInstance(DEFAULT_CIPHER_ALGORITHM);// 创建密码器

            byte[] byteContent = content.getBytes("utf-8");

            cipher.init(Cipher.ENCRYPT_MODE, getSecretKey(key));// 初始化为加密模式的密码器

            byte[] result = cipher.doFinal(byteContent);// 加密

            return org.apache.commons.codec.binary.Base64.encodeBase64String(result);//通过Base64转码返回
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return null;
    }

    /**
     * AES 解密操作
     *
     * @param content
     * @param key
     * @return
     */
    public static String decrypt(String content, String key) {

        try {
            //实例化
            Cipher cipher = Cipher.getInstance(DEFAULT_CIPHER_ALGORITHM);

            //使用密钥初始化，设置为解密模式
            cipher.init(Cipher.DECRYPT_MODE, getSecretKey(key));

            //执行操作
            byte[] result = cipher.doFinal(org.apache.commons.codec.binary.Base64.decodeBase64(content));

            return new String(result, "utf-8");
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return null;
    }

    /**
     * 生成加密秘钥
     *
     * @return
     * @throws UnsupportedEncodingException
     */
    private static SecretKeySpec getSecretKey(final String key) throws UnsupportedEncodingException {
        //返回生成指定算法密钥生成器的 KeyGenerator 对象
        KeyGenerator kg = null;

        try {
            kg = KeyGenerator.getInstance(KEY_ALGORITHM);

            //AES 要求密钥长度为 128
            kg.init(128, new SecureRandom(key.getBytes()));

            //生成一个密钥
            SecretKey secretKey = kg.generateKey();

            return new SecretKeySpec(Arrays.copyOf(key.getBytes("utf-8"), 16), KEY_ALGORITHM);// 转换为AES专用密钥
        } catch (NoSuchAlgorithmException ex) {
            ex.printStackTrace();
        }

        return null;
    }


}
