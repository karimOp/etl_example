package org.klab.com.etl;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.commons.lang.StringUtils;

/**
 * Hello world!
 *
 */
public class Movies {
    String SEPARATOR = "\t";
    int GLOBAL_NULL_COUNT = 0;
    String GLOBAL_EXTENSION = ".csv";
    int GLOBAL_N = 0;
    String MOVIES_PATH = "/Users/albertoacuna/Documents/klab/Telefonica/data/Datos/Movies/";
    String MOVIE_HEADER = "LOCALOB\tDATE\tTABS\tCONTENTID\tPROVIDERID\tCONTENTTITLE\tSTATUS\tLICENSESTART\tLICENSEEND\tSTATUSDATE\tDEVICES\tDISTRIBUTOR\tPRODUCER\tGENRES\tLANGUAGES\tSUBTITLES\tCOMMERCIALIZATIONTYPE\tCONTENTCATEGORY\tTVODTYPE\tMOVIETYPE\tORIGTITLE\tCOUNTRY\tRELEASEDATE\tDURATION\tACTOR\tDIRECTOR\tOBINSTANCECODE";
    String MASTER_LAYOUT = "CONTENTID\tPROVIDERID\tCONTENTTITLE\tSTATUS\tLICENSESTART\tLICENSEEND\tSTATUSDATE\tDEVICES\tDISTRIBUTOR\tPRODUCER\tGENRES\tLANGUAGES\tSUBTITLES\tCOMMERCIALIZATIONTYPE\tCONTENTCATEGORY\tTVODTYPE\tMOVIETYPE\tORIGTITLE\tCOUNTRY\tRELEASEDATE\tDURATION\tACTOR\tDIRECTOR\tOBINSTANCECODE";;
    ArrayList<String> HEADERS;
    
    /**
     * This quick and dirty Java class is extracting and matching the different Movie layout we previously identified within the data.
     * Using HEADERS -> getHeaderStructure
     * We are using this approach thanks to apache common lang + io, fastest library to open, access and close several files
     * */
    
    public Movies() {
	HEADERS = new ArrayList<String>();
    }

    /* Getting all documents from the Path */
    @SuppressWarnings("rawtypes")
    Collection getListOfAllConfigFiles(String directoryName) {
	File directory = new File(directoryName);
	return FileUtils.listFiles(directory, new WildcardFileFilter("*INJ.csv"), null);
    }

    public void readFile(String file, BufferedWriter bw) {
	int _n_ = 0;
	int _r_ = 0;
	LineIterator it = null;
	String FILE_DATE = movieFileDate(file);
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

    public String fixLine(String line) {
	// First getting the tokens
	// tabCount will identify the origination layout and will map that data into the MasterLayout
	String[] masterLayout = new String[24];
	
	String internal = new String(line);
	String[] token = internal.split(SEPARATOR);
	
	if(token.length==24){
	//ETL3 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 NULL
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
	    masterLayout[13]=!token[13].isEmpty()?token[13]:"NULL";
	    masterLayout[14]=!token[14].isEmpty()?token[14]:"NULL";
	    
	    masterLayout[15]=!token[15].isEmpty()?token[15]:"NULL";
	    masterLayout[16]=!token[16].isEmpty()?token[16]:"NULL";
	    masterLayout[17]=!token[17].isEmpty()?token[17]:"NULL";
	    masterLayout[18]=!token[18].isEmpty()?token[18]:"NULL";
	    masterLayout[19]=!token[19].isEmpty()?token[19]:"NULL";
	    
	    masterLayout[20]=!token[20].isEmpty()?token[20]:"NULL";
	    masterLayout[21]=!token[21].isEmpty()?token[21]:"NULL";
	    masterLayout[22]=!token[22].isEmpty()?token[22]:"NULL";
	    masterLayout[23]=!token[23].isEmpty()?token[23]:"NULL";
	    
	}else if(token.length==18){
	 //ETL2 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 NULL NULL NULL NULL NULL NULL 18 NULL
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
	    masterLayout[13]=!token[13].isEmpty()?token[13]:"NULL";
	    masterLayout[14]=!token[14].isEmpty()?token[14]:"NULL";
	    
	    masterLayout[15]=!token[15].isEmpty()?token[15]:"NULL";
	    masterLayout[16]=!token[16].isEmpty()?token[16]:"NULL";
	    masterLayout[17]="NULL";
	    masterLayout[18]="NULL";
	    masterLayout[19]="NULL";
	    
	    masterLayout[20]="NULL";
	    masterLayout[21]="NULL";
	    masterLayout[22]="NULL";
	    masterLayout[23]=!token[17].isEmpty()?token[17]:"NULL";
	}else if(token.length==12){
	    //ETL1 1 2 3 4 5 6 7 NULL 9 10 11 NULL NULL NULL NULL NULL NULL NULL NULL NULL NULL NULL NULL 12 8
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
	    masterLayout[11]="NULL";
	    masterLayout[12]="NULL";
	    masterLayout[13]="NULL";
	    masterLayout[14]="NULL";
	    
	    masterLayout[15]="NULL";
	    masterLayout[16]="NULL";
	    masterLayout[17]="NULL";
	    masterLayout[18]="NULL";
	    masterLayout[19]="NULL";
	    
	    masterLayout[20]="NULL";
	    masterLayout[21]="NULL";
	    masterLayout[22]="NULL";
	    masterLayout[23]=!token[11].isEmpty()?token[11]:"NULL";
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

    public String movieFileDate(String FullPath) {
	String nameOfTheFile = FullPath.replace(MOVIES_PATH, "");
	return nameOfTheFile.substring(7, 15);
    }
    
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
    
    public static void main(String[] args) throws IOException {
	Movies app = new Movies();
	@SuppressWarnings("unchecked")
	ArrayList<File> listOfFiles = new ArrayList<File>(app.getListOfAllConfigFiles(app.MOVIES_PATH));
	System.out.println("Amount of Files: "+listOfFiles.size());
	File fout = new File(app.MOVIES_PATH+"Movies_all.csv");
	FileOutputStream fos = new FileOutputStream(fout);
 	BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
 	bw.write(app.MOVIE_HEADER);
 	bw.newLine();
 	//Layout 1: Movies_01012014_INJ.csv
 	//Layout 2: Movies_14032014_INJ.csv
 	//Layout 3: Movies_31102014_INJ.csv
 	for (File file : listOfFiles) {
 	   System.out.println("Processing: "+file.getAbsolutePath().replace(app.MOVIES_PATH,""));
 	   app.readFile(file.getAbsolutePath(),bw);
	}
	bw.close();
    }
}
