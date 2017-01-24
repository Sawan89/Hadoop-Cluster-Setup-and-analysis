import java.net.*;
import java.io.*;
import java.util.*;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

public class XMLReaderForFlume {
  private static XMLReaderForFlume instance = null;
  private XMLReaderForFlume() {
  }
  public static XMLReaderForFlume getInstance() {
    if(instance == null) {
      instance = new XMLReaderForFlume();
    }
    return instance;
  }

  public void writeXML() {
    try {
    	URL url = new URL("http://egauge22162.egaug.es/57A0C/cgi-bin/egauge?tot&inst");
    	String reading = "";
    	String i_tag = "";
    	String t_tag = "";
    	String n_tag = "";
    	String previousTimeStamp = "";
    	String timeStamp = "";
    	File myFoo = new File("C:\\Users\\Sawan\\outputstream\\foo.txt");
    	FileWriter fooWriter = new FileWriter(myFoo, false); // true to append
    	                                                     // false to overwrite.
    	
    	
    	while(true){
    		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
	    	DocumentBuilder db = dbf.newDocumentBuilder();
	    	Document doc = db.parse(url.openStream());
	    	NodeList nodes = doc.getElementsByTagName("r");
	    	timeStamp = doc.getElementsByTagName("ts").item(0).getTextContent();
//	    	System.out.println("previousTimeStamp"+ previousTimeStamp +"timeStamp"+timeStamp);
	    	if(!(previousTimeStamp.equalsIgnoreCase(timeStamp))){
		    	for(int i=0;i<nodes.getLength();i++) {
		            Element element = (Element)nodes.item(i);
		            
					reading = element.getElementsByTagName("v").item(0).getTextContent();
					i_tag   = element.getElementsByTagName("i").item(0).getTextContent();
					t_tag = element.getAttribute("t");
					n_tag = element.getAttribute("n");
		            System.out.println(t_tag + " :" + n_tag + " :" + reading + " :" + i_tag + " :" + timeStamp);
		            fooWriter.write(t_tag + " :" + n_tag + " :" + reading + " :" + i_tag + " :" + timeStamp + "\n");
		        	

		          }
	    	}
	    	previousTimeStamp = timeStamp;
	    	
	    }
    	

    } catch(Exception ex) {
      ex.printStackTrace();
    }
  }
 
  
  public static void main(String[] args) {
    XMLReaderForFlume reader = XMLReaderForFlume.getInstance();
    reader.writeXML();
  }
}
