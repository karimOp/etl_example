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

public class Users {
    String SEPARATOR = "\t";
    int GLOBAL_NULL_COUNT = 0;
    String GLOBAL_EXTENSION = ".csv";
    int GLOBAL_N = 0;
    String USERS_PATH = "/Users/albertoacuna/Documents/klab/Telefonica/data/Datos/Users/";
    String USER_HEADER = "LOCALOB\tDATE\tTABS\tUNIQUEUSERCODE\tCREATIONDATE\tMIBID\tSTATUS\tSUBSCRIPTIONS\tUNIQUEUSERCODE_OLD";
    ArrayList<String> HEADERS;
    
    public Users(){
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
    
    
    public String fixLine(String line) {
	// First getting the tokens
	// tabCount will identify the origination layout and will map that data into the MasterLayout
	/*
	* UNIQUEUSERCODE CREATIONDATE MIBID STATUS UNIQUEUSERCODE_OLD
	* UNIQUEUSERCODE CREATIONDATE MIBID STATUS SUBSCRIPTIONS UNIQUEUSERCODE_OLD
	**/
	String[] masterLayout = new String[6];
	
	String internal = new String(line);
	String[] token = internal.split(SEPARATOR);
	
	if(token.length==6){
	    masterLayout[0]=!token[0].isEmpty()?token[0]:"NULL";
	    masterLayout[1]=!token[1].isEmpty()?token[1]:"NULL";
	    masterLayout[2]=!token[2].isEmpty()?token[2]:"NULL";
	    masterLayout[3]=!token[3].isEmpty()?token[3]:"NULL";
	    masterLayout[4]=!token[4].isEmpty()?token[4]:"NULL";
	    masterLayout[5]=!token[5].isEmpty()?token[5]:"NULL";
	  
	}else if(token.length==5){
	    masterLayout[0]=!token[0].isEmpty()?token[0]:"NULL";
	    masterLayout[1]=!token[1].isEmpty()?token[1]:"NULL";
	    masterLayout[2]=!token[2].isEmpty()?token[2]:"NULL";
	    masterLayout[3]=!token[3].isEmpty()?token[3]:"NULL";
	    masterLayout[4]="NULL";
	    masterLayout[5]=!token[4].isEmpty()?token[4]:"NULL";
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
    
    /* Getting all documents from the Path */
    @SuppressWarnings("rawtypes")
    Collection getListOfAllConfigFiles(String directoryName) {
	File directory = new File(directoryName);
	return FileUtils.listFiles(directory, new WildcardFileFilter("*_COMPLETED.csv"), null);
    }
    
    public void readFile(String file, BufferedWriter bw) {
	int _n_ = 0;
	int _r_ = 0;
	LineIterator it = null;
	String FILE_DATE = userFileDate(file);
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
    
    public String userFileDate(String FullPath) {
	String nameOfTheFile = FullPath.replace(USERS_PATH, "");
	return nameOfTheFile.substring(11, 19);
    }
    
    public static void main(String[] args) throws IOException {
	Users app = new Users();
	ArrayList<File> listOfFiles = new ArrayList<File>(app.getListOfAllConfigFiles(app.USERS_PATH));
	/*
	for (File in : listOfFiles) {
	    app.readinHeader(in);
	}
	
	for (String inside : app.HEADERS) {
	    System.out.println(inside);
	}
	*/
	System.out.println("Amount of Files: "+listOfFiles.size());
	File fout = new File(app.USERS_PATH+"Users_all.csv");
	FileOutputStream fos = new FileOutputStream(fout);
 	BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
 	bw.write(app.USER_HEADER);
 	bw.newLine();
 	
 	/*
 	app.readFile(app.USERS_PATH+"DONE_Users_01012014_COMPLETED.csv", bw);
 	app.readFile(app.USERS_PATH+"DONE_Users_31102014_COMPLETED.csv", bw);
 	*/
 	
 	int i=1;
 	for (File file : listOfFiles) {
 	    System.out.println("Processing "+i+":"+file.getAbsolutePath().replace(app.USERS_PATH, ""));
	    app.readFile(file.getAbsolutePath(), bw);
	    i++;
	}
 	
 	
 	bw.close();
	
	
    }
}
