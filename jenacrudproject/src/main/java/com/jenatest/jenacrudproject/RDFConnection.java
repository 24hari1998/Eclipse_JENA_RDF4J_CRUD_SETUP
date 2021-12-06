package com.jenatest.jenacrudproject;


import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.UnsupportedRDFormatException;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;


public class RDFConnection {

private Repository repo;
	
	public RDFConnection( )
	{
		Path path = Paths.get(".").toAbsolutePath().normalize();      
		String dbDir = path.toFile().getAbsolutePath() + "/dbRDF/"; 
		File dataDir = new File(dbDir);
		repo = new SailRepository(new NativeStore(dataDir));
		repo.init();
	}
	
	public void addStatementRDF(String data )
	{
		
		InputStream input = new ByteArrayInputStream(data.getBytes());
		
		
		
		
		
		try (RepositoryConnection conn = repo.getConnection()) {
			Model model = Rio.parse(input, "", RDFFormat.NTRIPLES);
			conn.add(model);
		
		}
	 catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		finally {
			// before our program exits, make sure the database is properly shut down.
			
			try {
				input.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			repo.shutDown();
		}
	}
	
	public void getStatements() {
		RDFWriter writer = null;
		FileOutputStream out = null;
		Path path = Paths.get(".").toAbsolutePath().normalize();      
		String dbDir = path.toFile().getAbsolutePath() + "/dbRDFLogs/"; 
		
		try {
			out = new FileOutputStream(dbDir + "file.nt", false);
			writer = Rio.createWriter(RDFFormat.NTRIPLES, out);
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		try (RepositoryConnection conn = repo.getConnection()) {
		
			try (RepositoryResult<Statement> result = conn.getStatements(null, null, null);) {
				 writer.startRDF();
				while (result.hasNext()) {
					Statement st = result.next();
					//System.out.println("db contains: " + st);
					writer.handleStatement(st);
					
				}
				writer.endRDF();
			
			}
		}
		
		try {
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
		
	
	
}
