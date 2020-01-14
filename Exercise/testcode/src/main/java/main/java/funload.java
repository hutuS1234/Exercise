package main.java;

import com.mysql.jdbc.Connection;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import java.io.FileNotFoundException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.lang.NullPointerException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.logging.*;
import java.net.ConnectException;

public class funload {
    private static final Logger LOGGER = Logger.getLogger(funload.class.getName());
    public static void data_loader(String cc, String ic, String format, int start, int end) throws IOException, JSONException, SAXException, ParserConfigurationException, NullPointerException, FileNotFoundException,ConnectException {

        if (format.equals("json")) {
            try {
                JSONArray json1, json3 = new JSONArray();
                json1 = json.readJsonFromUrl("https://api.worldbank.org/v2/country/" + cc + "/indicator/" + ic + "?scale=y&format=" + format + "&date=" + start + ":" + end + "");
                JSONObject json2, json4, json5, json6 = new JSONObject();

                json2 = (JSONObject) json1.get(0);
                json3 = (JSONArray) json1.get(1);
                boolean flag = false;

                int page_total = (Integer) json2.get("pages");
                int count = (Integer) (json2.get("per_page"));

                int total = (Integer) (json2.get("total"));
                if (total < count) count = total;
                try{
                    Class.forName("com.mysql.jdbc.Driver");//"com.mysql.jdbc.Driver"

                    Connection con = (Connection) DriverManager.getConnection("jdbc:mysql://localhost:3306/dbname", "root", "root");


                    for (int j = 1; j <= page_total; j++)
                    {
                        if (j <= page_total && j != 1 && flag == false) //shouldn't go in if total page and current page is both 1
                        {
                            flag = true;
                            json1 = json.readJsonFromUrl("https://api.worldbank.org/v2/country/" + cc + "/indicator/" + ic + "?scale=y&format=" + format + "&date=" + start + ":" + end + "&page=" + j);
                            json2 = (JSONObject) json1.get(0);
                            json3 = (JSONArray) json1.get(1);
                            if (j == page_total)
                            {
                               count = total - ((page_total - 1) * count);
                            }

                        }
                        for (int i = 0; i <= count - 1; i++)
                        {
                            json4 = (JSONObject) json3.get(i);
                            json5 = (JSONObject) (json4.get("indicator"));
                            json6 = (JSONObject) (json4.get("country"));
                            DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                            Calendar cal = Calendar.getInstance();

                            try
                            {

                                Statement stmt = con.createStatement();
                                ResultSet rs = stmt.executeQuery("select * from datatable");
                                boolean inflag=false;
                                while (rs.next())
                                {
                                    if (rs.getString(6) == (String) json4.get("date") && rs.getString(5) == (String) json4.get("countryiso3code") )
                                    {
                                        if (rs.getString(1) != (String) json5.get("id"))
                                        {
                                            stmt.executeUpdate("update table datatable set indicator_id="+(String) json5.get("id") +" where countryiso3code="+(String)json4.get("countryiso3code")+" and dDate="+(String)json4.get("date")+";");
                                        }
                                        inflag=true;
                                        break;
                                    }
                                }
                                if(inflag=false)
                                {
                                        stmt.executeUpdate("insert into datatable (run_datetime ,indicator_id ,indicator_value  ,country_id ," +
                                                "country_value ,countryiso3code,dDate ,dValue ,dScale ,Unit ,obs_status ,dDecimal) values(" + (dateFormat.format(cal.getTime())) + "," + (String) json5.get("id") + "," + (String) json5.get("value") + "," +
                                                (String) json6.get("id") + "," + (String) json6.get("value") + "," + (String) json4.get("countryiso3code") + "," + (String) json4.get("date") + "," + (Integer) json4.get("value") +","+(String) json4.get("scale")+
                                                "," + (String) json4.get("unit") + "," + (String) json4.get("obs_status") + "," + (Integer) json4.get("decimal")  + ");");

                                }

                            }

                            catch (SQLException e)
                            {
                                e.printStackTrace();
                                LOGGER.log(Level.SEVERE, "Exception occur", e);
                            }

                            catch (Exception e)
                            {
                                 e.printStackTrace();
                                LOGGER.log(Level.SEVERE, "Exception occur", e);
                            }

                            if (i == (count - 1))
                            {
                                flag = false;
                            }
                        }
                    }
                    con.close();
                }
                catch (ClassNotFoundException e)
                {
                        e.printStackTrace();
                        LOGGER.log(Level.SEVERE, "Exception occur", e);
                }

            }
            catch (Exception e)
            {
                System.out.println(e);
                LOGGER.log(Level.SEVERE, "Exception occur", e);
            }


        }

//        if (format.equals("xml")) {
//            try {
//
//
//                URL url = new URL("https://api.worldbank.org/v2/country/" + cc + "/indicator/" + ic + "?scale=y&format=" + format + "&date=" + start + ":" + end);
//                HttpURLConnection con = (HttpURLConnection) url.openConnection();
//                int responseCode = con.getResponseCode();
//
//                BufferedReader in = new BufferedReader(
//                        new InputStreamReader(con.getInputStream()));
//                String inputLine;
//                StringBuffer response = new StringBuffer();
//                while ((inputLine = in.readLine()) != null) {
//                    response.append(inputLine);
//                }
//                in.close();
//                String response1 = response.toString();
//
//                response1 = response1.substring(1); //resolving problem in API response. referred https://stackoverflow.com/questions/11577420/fatal-error-11-content-is-not-allowed-in-prolog for error
//                Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder()
//                        .parse(new InputSource(new StringReader(response1)));
//
//
//                NodeList Node = doc.getElementsByTagName("wb:data");
//
//                if (Node.getLength() > 0) {
//                    Element ele = (Element)Node.item(0);
//
//                    boolean flag=false;
//                    String spage_total=ele.getAttribute("pages");
//
//                    int page_total=Integer.parseInt(spage_total);
//                    String scount=ele.getAttribute("per_page");
//
//                    int count=Integer.parseInt(scount);
//                    String stotal=ele.getAttribute("total");
//
//                    int total=Integer.parseInt(stotal);
//                    if (total < count) count = total;
//                    NodeList Node2=(NodeList) doc.getChildNodes();
//
//                        for (int i = 0; i <= count - 1; i++) {
//                            //Node subnode=(Node) Node2;
//                            Element eElement = (Element) Node2.item(0);
//                            String svalue = eElement.getElementsByTagName("wb:date").item(0).getTextContent();
//                            System.out.println(svalue);
//                            Node2=(NodeList) doc.getNextSibling();
//
//
//                            if (i == (count - 1)) {
//                                flag = false;
//                            }
//
//                        }
//                }
//            } catch (Exception e) {
//                System.out.println(e);
//            }
//        }
    }
}



