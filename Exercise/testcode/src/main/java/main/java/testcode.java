package main.java;

import org.json.JSONException;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;

public class testcode {
    public static void main(String[] args) throws IOException, JSONException, ParserConfigurationException, SAXException {
        funload.data_loader("ind","SP.POP.TOTL","json",1960,2018);
    }

}
