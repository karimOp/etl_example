package org.klab.com.etl;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.commons.lang.StringUtils;

public class Consumption {
    String SEPARATOR = "\t";
    int GLOBAL_NULL_COUNT = 0;
    String GLOBAL_EXTENSION = ".csv";
    int GLOBAL_N = 0;
    String CONSUMP_PATH = "/Users/albertoacuna/Documents/klab/Telefonica/data/Datos/ConsumptionVoD/";
    String CONSUMP_HEADER = "LOCALOB\tDATE\tTABS\tDATETIME\tOBINSTANCECODE\tUSERTYPE\tUNIQUEUSERCODE\tDEVICETYPEUSED\tDEVICEID\tMOVIEID\tMEDIAID\tSUBSCRIPTIONID\tCURRENTHEARTBEAT\tCOMMERCIALIZATIONTYPEID\tCONTENTCATEGORYID\tTVODTYPEID";
    ArrayList<String> HEADERS;
    
    public Consumption(){
	HEADERS = new ArrayList<String>();
    }
    // HEADER REVIEW
    public void getHeaderStructure(String Header){
	if(!HEADERS.contains(Header)){
	    HEADERS.add(Header);
	}
    }
    
    public void readinHeader(File file){
 	LineIterator it = null;
 	try {
 	    it = FileUtils.lineIterator(file, "UTF-8");
 	    if(it.hasNext()){
 		String line = it.nextLine();
 		getHeaderStructure(line);
 	    }
 	} catch (IOException e) {
 	    // TODO Auto-generated catch block
 	    e.printStackTrace();

 	} finally {
 	    LineIterator.closeQuietly(it);
 	}
     }
    
    /* Getting all documents from the Path */
    @SuppressWarnings("rawtypes")
    Collection getListOfAllConfigFiles(String directoryName) {
	File directory = new File(directoryName);
	return FileUtils.listFiles(directory, new WildcardFileFilter("*INJ.csv"), null);
    }
    /**
 	DATETIME OBINSTANCECODE USERTYPE UNIQUEUSERCODE DEVICETYPEUSED DEVICEID MOVIEID MEDIAID	SUBSCRIPTIONID CURRENTHEARTBEAT
	DATETIME OBINSTANCECODE USERTYPE UNIQUEUSERCODE DEVICETYPEUSED DEVICEID MOVIEID MEDIAID	SUBSCRIPTIONID CURRENTHEARTBEAT COMMERCIALIZATIONTYPEID CONTENTCATEGORYID TVODTYPEID
     * 
     * */
    
    public String fixLine(String line) {
	// First getting the tokens
	// tabCount will identify the origination layout and will map that data into the MasterLayout
	String[] masterLayout = new String[13];
	
	String internal = new String(line);
	String[] token = internal.split(SEPARATOR);
	
	if(token.length==13){
	    masterLayout[0]=!token[0].isEmpty()?token[0]:"NULL";
	    masterLayout[1]=!token[1].isEmpty()?token[1]:"NULL";
	    masterLayout[2]=!token[2].isEmpty()?token[2]:"NULL";
	    masterLayout[3]=!token[3].isEmpty()?token[3]:"NULL";
	    masterLayout[4]=!token[4].isEmpty()?token[4]:"NULL";
	    
	    masterLayout[5]=!token[5].isEmpty()?token[5]:"NULL";
	    masterLayout[6]=!token[6].isEmpty()?token[6]:"NULL";
	    masterLayout[7]=!token[7].isEmpty()?token[7]:"NULL";
	    masterLayout[8]=!token[8].isEmpty()?token[8]:"NULL";
	    masterLayout[9]=!token[9].isEmpty()?token[9]:"NULL";
	    
	    masterLayout[10]=!token[10].isEmpty()?token[10]:"NULL";
	    masterLayout[11]=!token[11].isEmpty()?token[11]:"NULL";
	    masterLayout[12]=!token[12].isEmpty()?token[12]:"NULL";
	  
	}else if(token.length==10){
	    masterLayout[0]=!token[0].isEmpty()?token[0]:"NULL";
	    masterLayout[1]=!token[1].isEmpty()?token[1]:"NULL";
	    masterLayout[2]=!token[2].isEmpty()?token[2]:"NULL";
	    masterLayout[3]=!token[3].isEmpty()?token[3]:"NULL";
	    masterLayout[4]=!token[4].isEmpty()?token[4]:"NULL";
	    
	    masterLayout[5]=!token[5].isEmpty()?token[5]:"NULL";
	    masterLayout[6]=!token[6].isEmpty()?token[6]:"NULL";
	    masterLayout[7]=!token[7].isEmpty()?token[6]:"NULL";
	    masterLayout[8]=!token[8].isEmpty()?token[8]:"NULL";
	    masterLayout[9]=!token[9].isEmpty()?token[9]:"NULL";
	    
	    masterLayout[10]="NULL";
	    masterLayout[11]="NULL";
	    masterLayout[12]="NULL";
	    
	}else{
	    System.out.println("Observation rejected :(");
	    System.out.println(line);
	    return "";
	}
	     
	
	String fixed = "";
	for (int j = 0; j < masterLayout.length - 1; j++) {
	    fixed = fixed + masterLayout[j] + SEPARATOR;
	}
	fixed = fixed + masterLayout[masterLayout.length - 1];

	return fixed;
    }
    
    public String consumpFileDate(String FullPath) {
	String nameOfTheFile = FullPath.replace(CONSUMP_PATH, "");
	return nameOfTheFile.substring(20, 30);
    }
    
    
    public void readFile(String file, BufferedWriter bw) {
	int _n_ = 0;
	int _r_ = 0;
	LineIterator it = null;
	String FILE_DATE = consumpFileDate(file);
	try {
	    it = FileUtils.lineIterator(new File(file), "UTF-8");
	    while (it.hasNext()) {
		String line = it.nextLine();
		
		int amountOfTabsLine = StringUtils.countMatches(line,SEPARATOR);
		//This will decide the structure
		String fixedLine = fixLine(line);
		// do something with line
		// System.out.println("N:\t"+amountOfTabsLine+ "\t"+line);
		if(!fixedLine.isEmpty()&&_n_>0){
		    String output = _n_+"\t"+FILE_DATE+"\t"+amountOfTabsLine + "\t" + fixedLine;
		    bw.write(output);
		    bw.newLine();
		    _r_++;
		}
		
		_n_++;
	    }

	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();

	} finally {
	    System.out.println("io > ["+_n_+"]["+_r_+"] > out");
	    LineIterator.closeQuietly(it);
	}
    }
    
    
    public static void main(String[] args) throws IOException {
	Consumption app = new Consumption();
	ArrayList<File> listOfFiles = new ArrayList<File>(app.getListOfAllConfigFiles(app.CONSUMP_PATH));
	/*
	for (File in : listOfFiles) {
	    app.readinHeader(in);
	}
	
	for (String inside : app.HEADERS) {
	    System.out.println(inside);
	}
	*/
	System.out.println("Amount of Files: "+listOfFiles.size());
	File fout = new File(app.CONSUMP_PATH+"Consump_all.csv");
	FileOutputStream fos = new FileOutputStream(fout);
 	BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
 	bw.write(app.CONSUMP_HEADER);
 	bw.newLine();
 	int i=1;
 	for (File file : listOfFiles) {
 	    System.out.println("Processing "+i+":"+file.getAbsolutePath().replace(app.CONSUMP_PATH, ""));
	    app.readFile(file.getAbsolutePath(), bw);
	    i++;
	}
 	
 	
 	bw.close();
	
    }

}
