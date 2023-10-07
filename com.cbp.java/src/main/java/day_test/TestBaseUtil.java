package day_test;

import org.apache.commons.beanutils.BeanUtils;

/**
 * @ProjectName: my_studay
 * @Desciption:
 * @Author: changbp
 * @Date: 2023/8/31 11:28
 */
public class TestBaseUtil {
    public static void main(String[] args) {
        // 创建源对象
        Person source = new Person("John", 25);

        // 创建目标对象
        Person destination = new Person();

        try {
            // 将源对象的属性值复制到目标对象中
            BeanUtils.copyProperties(source, destination);

            // 输出目标对象的属性
            System.out.println("姓名: " + destination.getName());
            System.out.println("年龄: " + destination.getAge());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
class Person {
    private String name;
    private int age;

    public Person() {}

    public Person(String name, int age) {
        this.name = name;
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }
}
