package com.cbp.util;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.InputStream;

/**
 * @Author changbp
 * @Date 2021-06-04 16:00
 * @Return
 * @Version 1.0
 */
public class DomReadXML {
    public static void main(String[] args) {
        final InputStream resourceAsStream = DomReadXML.class.getClassLoader().getResourceAsStream("book.xml");
        try {
            final DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            final Document document = documentBuilder.parse(resourceAsStream);
            final NodeList nodeList = document.getElementsByTagName("book");
            Element e = (Element) nodeList.item(0);
            final String name = e.getElementsByTagName("name").item(0).getFirstChild().getNodeValue();
            final String isbn = e.getElementsByTagName("isbn").item(0).getFirstChild().getNodeValue();
            final String author = e.getElementsByTagName("author").item(0).getFirstChild().getNodeValue();
            System.out.println(name);
            System.out.println(isbn);
            System.out.println(author);
        } catch (ParserConfigurationException | IOException | SAXException e) {
            e.printStackTrace();
        }
    }
}
