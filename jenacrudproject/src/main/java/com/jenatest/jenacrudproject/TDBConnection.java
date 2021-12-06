package com.jenatest.jenacrudproject;

import java.io.File;
import java.io.StringReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;


import org.apache.jena.graph.Graph;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.tdb.TDBFactory;
import org.apache.jena.tdb.base.file.Location;
import org.apache.jena.tdb2.DatabaseMgr;
import org.apache.jena.tdb2.TDB2Factory;






public class TDBConnection {
	
private Dataset ds;

	
	public TDBConnection( )
	{
		Path path = Paths.get(".").toAbsolutePath().normalize();      
		String dbDir = path.toFile().getAbsolutePath() + "/db1/"; 
		Location location = Location.create(dbDir); 
		
		/* Change to TDB2Factory for TDB2 */
		ds = TDBFactory.createDataset(location); 
	}
	
	
	public void compactCall() {
		DatabaseMgr.compact(ds.asDatasetGraph(), false);
	}

	
	/**
	 * Request triples from the TDB back-end
	 * @param modelName - graph identifier
	 * @return statements that fit the s-p-o description
	 */
	public List<Statement> getStatements( String modelName, String subject, String property, String object )
	{
		List<Statement> results = new ArrayList<Statement>();
		
		Model model = null;
		
		ds.begin( ReadWrite.READ );
		try
		{
			model = ds.getNamedModel( modelName );
			
			
			
			
			
			ds.commit();
		}
		finally
		{
			if( model != null ) model.close();
			ds.end();
		}
		
		return results;
	}
	
	/**
	 * Add a statement to the TDB back-end for the given UserSession Model
	 * @param modelName - graph identifier
	 */
	public void addStatement( String data )
	{
		
		Model model = null;
		StringReader reader = new StringReader(data);
		
		ds.begin( ReadWrite.WRITE );
		try
		{
			model = ModelFactory.createDefaultModel();
			RDFDataMgr.read(model, reader, null, Lang.N3);
			Model modelS = ds.getDefaultModel();
			Graph repositoryGraph = modelS.getGraph();
			
			StmtIterator it = model.listStatements();
			while(it.hasNext()) {
				Statement stmt = it.nextStatement();
				repositoryGraph.add(stmt.asTriple() );
			}
			
			ds.commit();
		}
		finally
		{
			if( model != null ) model.close();
			ds.end();
		}
	}
	
	
	
	/**
	 * Remove a statement to the TDB Backend for the given UserSession Model
	 * @param modelName - graph identifier
	 */
	public void removeStatement( String modelName, String subject, String property, String object )
	{
		Model model = null;
		
		ds.begin( ReadWrite.WRITE );
		try
		{
			model = ds.getNamedModel( modelName );
			
			Statement stmt = model.createStatement
							 ( 	
								model.createResource( subject ), 
								model.createProperty( property ), 
								model.createResource( object ) 
							 );
					
			model.remove( stmt );
			ds.commit();
		}
		finally
		{
			if( model != null ) model.close();
			ds.end();
		}
	}
	
	/**
	 * Close the dataset
	 */
	public void close()
	{
		ds.close();
	}

}
