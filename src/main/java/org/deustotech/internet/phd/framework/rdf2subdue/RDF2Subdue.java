package org.deustotech.internet.phd.framework.rdf2subdue;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.RDFNode;
import org.apache.http.client.utils.URIBuilder;
import virtuoso.jena.driver.VirtGraph;
import virtuoso.jena.driver.VirtuosoQueryExecution;
import virtuoso.jena.driver.VirtuosoQueryExecutionFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Created by memaldi on 14/06/14.
 */
public class RDF2Subdue {

    public void launch(String dataset) {
        Logger logger = Logger.getLogger(RDF2Subdue.class.getName());
        logger.info("Initializing...");
        Properties prop = new Properties();
        InputStream input;
        try {
            input = RDF2Subdue.class.getResourceAsStream("/config.properties");
            prop.load(input);
            input.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        URI connectionURL = null;
        try {
            connectionURL = new URIBuilder().setScheme("jdbc:virtuoso").setHost(prop.getProperty("virtuoso_host"))
                    .setPort(Integer.parseInt(prop.getProperty("virtuoso_port"))).build();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

        VirtGraph graph = new VirtGraph("http://" + dataset, connectionURL.toString(), prop.getProperty("virtuoso_user"), prop.getProperty("virtuoso_password"));
        Query query = QueryFactory.create("SELECT DISTINCT ?s ?p ?o WHERE { ?s ?p ?o }");
        VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(query, graph);
        ResultSet results = vqe.execSelect();
        logger.info("Iterating dataset...");
        while (results.hasNext()) {
            QuerySolution result = results.next();
            RDFNode subject = result.get("s");
            RDFNode predicate = result.get("p");
            RDFNode object = result.get("o");
        }
        logger.info("Done!");
    }
}
