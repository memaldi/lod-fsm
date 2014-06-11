package org.deustotech.internet.phd.baselines;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.vocabulary.RDF;
import javafx.util.Pair;
import org.apache.http.client.utils.URIBuilder;
import virtuoso.jena.driver.VirtGraph;
import virtuoso.jena.driver.VirtuosoQueryExecution;
import virtuoso.jena.driver.VirtuosoQueryExecutionFactory;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

/**
 * Created by mikel on 11/06/14.
 */
public class DistinctTriplesEqualityBaseline {
    public void launch(String csvLocation) {
        Properties prop = new Properties();
        InputStream input = null;
        try {
            input = getClass().getResourceAsStream("/config.properties");
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
        BufferedReader br = null;
        Set<String> datasetList = new HashSet<String>();
        try {
            br = new BufferedReader(new FileReader(csvLocation));
            String line;
            while ((line = br.readLine()) != null) {
                String[] sline = line.split(",");
                datasetList.add(sline[4]);
            }
            br.close();
            Set<String> doneList = new HashSet<String>();
            Map<String, Map<String, Integer>> commonMap = new HashMap<String, Map<String, Integer>>();
            for (String sourceDataset : datasetList) {
                Map<String, Integer> map = new HashMap<String, Integer>();
                VirtGraph graph = new VirtGraph("http://" + sourceDataset, connectionURL.toString(), prop.getProperty("virtuoso_user"), prop.getProperty("virtuoso_password"));
                Query query = QueryFactory.create("SELECT DISTINCT ?p ?o WHERE { ?s ?p ?o }");
                VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(query, graph);
                ResultSet results = vqe.execSelect();
                while(results.hasNext()) {
                    QuerySolution result = results.nextSolution();
                    RDFNode predicate = result.get("p");
                    RDFNode object = result.get("o");
                    for (String targetDataset : datasetList) {
                        if ((!doneList.contains(targetDataset)) && (!targetDataset.equals(sourceDataset))) {
                            VirtGraph targetGraph = new VirtGraph("http://" + targetDataset, connectionURL.toString(), prop.getProperty("virtuoso_user"), prop.getProperty("virtuoso_password"));
                            Query targetQuery = null;
                            if (object.isLiteral()) {
                                targetQuery = QueryFactory.create(String.format("ASK { ?s <%s> \"%s\"}", predicate, object));
                            } else {
                                targetQuery = QueryFactory.create(String.format("ASK { ?s <%s> <%s>}", predicate, object));
                            }
                            VirtuosoQueryExecution targetVqe = VirtuosoQueryExecutionFactory.create(targetQuery, targetGraph);
                            ResultSet targetResults = targetVqe.execSelect();
                            while(targetResults.hasNext()){
                                QuerySolution targetResult = targetResults.next();
                                RDFNode ask = targetResult.get("__ask_retval");
                                if (ask.toString().equals("1^^http://www.w3.org/2001/XMLSchema#integer")) {
                                    int count = 0;
                                    if (map.containsKey(targetDataset)) {
                                        count = map.get(targetDataset);
                                    }
                                    map.put(targetDataset, ++count);
                                }

                            }
                        }
                    }
                }
                commonMap.put(sourceDataset, map);
                doneList.add(sourceDataset);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
