package org.deustotech.internet.phd.baselines;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import net.ericaro.neoitertools.Generator;
import net.ericaro.neoitertools.Itertools;
import org.apache.http.client.utils.URIBuilder;
import virtuoso.jena.driver.VirtGraph;
import virtuoso.jena.driver.VirtuosoQueryExecution;
import virtuoso.jena.driver.VirtuosoQueryExecutionFactory;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.logging.Logger;

/**
 * Created by memaldi on 12/12/14.
 */
public class OntologyRankingBaseline {
    public void launch(String csvLocation) {
        Logger logger = Logger.getLogger(this.getClass().getName());
        logger.info("Initializing...");
        Properties prop = new Properties();
        InputStream input;

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

        BufferedReader br;
        Set<String> datasetList = new HashSet<String>();

        try {
            br = new BufferedReader(new FileReader(csvLocation));
            String line;
            while ((line = br.readLine()) != null) {
                String[] sline = line.split(",");
                datasetList.add(sline[4]);
            }
            br.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        Map<String, Map<String, Float>> datasetStats = new HashMap<>();

        for (String dataset : datasetList) {
            Map<String, Float> ontologyStats = new HashMap<>();
            VirtGraph graph = new VirtGraph("http://" + dataset, connectionURL.toString(), prop.getProperty("virtuoso_user"), prop.getProperty("virtuoso_password"));
            Query query = QueryFactory.create("select distinct ?Class count(?Class) as ?count where {[] a ?Class}");
            VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(query, graph);
            ResultSet results = vqe.execSelect();
            while (results.hasNext()) {
                QuerySolution result = results.next();
                String clazz = result.get("Class").toString();
                float count = Float.parseFloat(result.get("count").toString());

                String ontologyURI = getPrefix(clazz);
                float accum = 0;
                if (ontologyStats.containsKey(ontologyURI)) {
                    accum = ontologyStats.get(ontologyURI);
                }
                accum += count;
                ontologyStats.put(getPrefix(clazz), accum);
            }

            query = QueryFactory.create("select distinct ?property count(?property) as ?count where {?s ?property ?o}");
            vqe = VirtuosoQueryExecutionFactory.create(query, graph);
            results = vqe.execSelect();
            while (results.hasNext()) {
                QuerySolution result = results.next();
                String property = result.get("property").toString();
                float count = Float.parseFloat(result.get("count").toString());

                String ontologyURI = getPrefix(property);
                float accum = 0;
                if (ontologyStats.containsKey(ontologyURI)) {
                    accum = ontologyStats.get(ontologyURI);
                }
                accum += count;
                ontologyStats.put(getPrefix(property), accum);
            }

            datasetStats.put(dataset, ontologyStats);
        }

        Map<String, Map<String, Float>> datasetPercentStats = new HashMap<>();

        for (String dataset : datasetStats.keySet()) {
            int total = 0;
            Map<String, Float> ontologyStats = datasetStats.get(dataset);
            for (String ontology : ontologyStats.keySet()) {
                total += ontologyStats.get(ontology);
            }
            Map<String, Float> ontologyPercentStats = new HashMap<>();
            for (String ontology: ontologyStats.keySet()) {
                float count = ontologyStats.get(ontology);
                ontologyPercentStats.put(ontology, count / total);
            }

            datasetPercentStats.put(dataset, ontologyPercentStats);
        }

        Generator<List<String>> combinations = Itertools.combinations(Itertools.iter(datasetList.iterator()), 2);
        List<String> pair;

        try {
            while((pair = combinations.next()) != null) {
                String sourceDataset = pair.get(0);
                String targetDataset = pair.get(1);

                List<String> sourceOntologyList = new ArrayList<>();
                List<String> targetOntologyList = new ArrayList<>();

                Map<String, Float> sourceOntologyStats = datasetPercentStats.get(sourceDataset);
                Map<String, Float> targetOntologyStats = datasetPercentStats.get(targetDataset);

                //List<String> iterableList = new ArrayList<>();
                //List<String> objectiveList = new ArrayList<>();

                if (datasetPercentStats.get(sourceDataset).keySet().size() >= datasetPercentStats.get(targetDataset).keySet().size()) {
                    sourceOntologyList = new ArrayList<>(datasetPercentStats.get(sourceDataset));
                    targetOntologyList = new ArrayList<>(datasetPercentStats.get(targetDataset));
                } else {
                    sourceOntologyList = new ArrayList<>(datasetPercentStats.get(targetDataset));
                    targetOntologyList = new ArrayList<>(datasetPercentStats.get(sourceDataset));
                }

                for (String sourceOntology : sourceOntologyList) {
                    for (String targetOntology: targetOntologyList) {

                    }
                }

            }
        } catch (NoSuchElementException e) {
            // Well, combinations.next() do not return null when the last element is reached.
        }

    }

    private static String getPrefix(String uri) {
        if (uri.contains("#")) {
            return uri.split("#")[0] + "#";
        } else {
            String[] suri = uri.split("/");
            String result = "";
            for (int i=0; i < suri.length - 1; i++) {
                result += suri[i] + "/";
            }
            return result;
        }
    }
}
