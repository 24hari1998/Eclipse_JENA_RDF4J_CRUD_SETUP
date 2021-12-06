package com.jenatest.jenacrudproject;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;



public class TDBMain {
	
	public static void main(String[] args) 
	{
		// Object to create for testing RDF4J
		RDFConnection tdb1 = null;
		// Object to create for testing JENA
		TDBConnection tdb2 = null;
		
		String data = "";
		long count = 0;
		tdb1 = new RDFConnection();
		
		InputStream is = TDBMain.class.getClassLoader()
                .getResourceAsStream("Universities-1.nt");
		BufferedReader br = new BufferedReader (new InputStreamReader(is));
		try {
			br.readLine();
			br.readLine();
			while((data = br.readLine()) != null && count <= 50000) {
				int i;
				StringBuilder input = new StringBuilder();
				for(i = 0; i < 100; i++) {
					input.append(data);
					input.append(" \n");
					if((data = br.readLine()) == null)
						break;
				}
				
				tdb1.addStatementRDF(input.toString());
				/* Addition for JENA test */
				//tdb2.addStatement(input.toString());
				count += 100;
			}
			br.close();
			
		} catch (IOException e) {
			
			e.printStackTrace();
		}
		try {
			is.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		tdb1.getStatements();

	}


}
