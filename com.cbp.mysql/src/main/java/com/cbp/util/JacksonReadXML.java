package com.cbp.util;


import com.cbp.bean.Book;
import com.fasterxml.jackson.dataformat.xml.JacksonXmlModule;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import java.io.IOException;
import java.io.InputStream;

/**
 * @Author changbp
 * @Date 2021-06-04 17:59
 * @Return
 * @Version 1.0
 */
public class JacksonReadXML {
    /*
    * Jackson的开源的第三方库可以轻松做到XML到JavaBean的转换
    * */
    public static void main(String[] args) {
        InputStream input = JacksonReadXML.class.getClassLoader().getResourceAsStream("book.xml");
        JacksonXmlModule module = new JacksonXmlModule();
        XmlMapper mapper = new XmlMapper(module);
        Book book = null;
        try {
            book = mapper.readValue(input, Book.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(book.id);
        System.out.println(book.name);
        System.out.println(book.author);
        System.out.println(book.isbn);
        System.out.println(book.tags);
        System.out.println(book.pubDate);
    }


}
