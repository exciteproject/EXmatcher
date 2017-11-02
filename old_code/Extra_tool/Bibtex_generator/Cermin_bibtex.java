package behnam1;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import pl.edu.icm.cermine.bibref.CRFBibReferenceParser;
import pl.edu.icm.cermine.bibref.model.BibEntry;

public class testy {
	
	public static void main( String[] args ) throws Exception 
    {
        String csvFile = "./refrence_list.csv";
        BufferedReader br = null;
        String line = "";

        FileOutputStream fos = new FileOutputStream("./result_list.bib");
        PrintStream ps = new PrintStream(fos);
        System.setOut(ps);
        
            br = new BufferedReader(new FileReader(csvFile));
            while ((line = br.readLine()) != null) {

                // use comma as separator
            	String textt=line.replace("\"", "");
        		CRFBibReferenceParser parser = CRFBibReferenceParser.getInstance();
        		BibEntry bibEntry = parser.parseBibReference(textt);
            	System.out.println(bibEntry.toBibTeX());
            }



	
}
}