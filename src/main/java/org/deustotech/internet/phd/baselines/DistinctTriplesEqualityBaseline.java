package org.deustotech.internet.phd.baselines;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.RDFNode;
import net.ericaro.neoitertools.Generator;
import net.ericaro.neoitertools.Itertools;
import org.apache.http.client.utils.URIBuilder;
import org.apache.thrift.TException;
import org.deustotech.internet.phd.framework.matchsubgraphs.MatchSubgraphs;
import org.hypertable.thrift.ThriftClient;
import org.hypertable.thriftgen.Cell;
import org.hypertable.thriftgen.HqlResult;
import virtuoso.jena.driver.VirtGraph;
import virtuoso.jena.driver.VirtuosoQueryExecution;
import virtuoso.jena.driver.VirtuosoQueryExecutionFactory;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.logging.Logger;

/**
 * Created by mikel (m.emaldi at deusto dot es) (m.emaldi at deusto dot es) on 11/06/14.
 */
public class DistinctTriplesEqualityBaseline {

    private static String [] range = new String[] {"0.0", "0.1", "0.2", "0.3", "0.4", "0.5", "0.6", "0.7", "0.8", "0.9"};

    public static void run() {
        Logger logger = Logger.getLogger(DistinctTriplesEqualityBaseline.class.getName());
        logger.info("Initializing...");
        Properties prop = new Properties();
        InputStream input;
        try {
            input = DistinctTriplesEqualityBaseline.class.getResourceAsStream("/config.properties");
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
        Set<String> datasetList = new HashSet<String>();

        ThriftClient client = null;
        try {
            client = ThriftClient.create("localhost", 15867);
        } catch (TException e) {
            e.printStackTrace();
            System.exit(1);
        }

        long ns = 0;
        try {
            if (!client.namespace_exists("framework")) {
                client.namespace_create("framework");
            }
            ns = client.namespace_open("framework");
        } catch (TException e) {
            e.printStackTrace();
        }

        String hqlQuery = "SELECT * from subgraphs where type = 'vertex'";

        HqlResult hqlResult = null;
        try {
            hqlResult = client.hql_query(ns, hqlQuery);
            if (hqlResult.getCells().size() > 0) {
                for (Cell cell : hqlResult.getCells()) {
                    ByteBuffer graphBuffer = client.get_cell(ns, "subgraphs", cell.getKey().getRow(), "graph");
                    String graph = new String(graphBuffer.array(), graphBuffer.position(), graphBuffer.remaining());
                    if (!datasetList.contains(graph.replace(".g", ""))) {
                        datasetList.add(graph.replace(".g", ""));
                    }
                }
            }
        } catch (TException e) {
            e.printStackTrace();
        }

        Set<String> doneList = new HashSet<String>();
        Map<String, Map<String, Integer>> commonMap = new HashMap<String, Map<String, Integer>>();
        Map<String, Set<String>> exclusionMap = new HashMap<String, Set<String>>();
        Map<String, Integer> totalTripleMap = new HashMap<>();

        for (String sourceDataset : datasetList) {
            logger.info(String.format("Analyzing %s dataset...", sourceDataset));
            int totalTriples = 0;
            Map<String, Integer> map = new HashMap<String, Integer>();
            assert connectionURL != null;
            VirtGraph graph = new VirtGraph("http://" + sourceDataset, connectionURL.toString(), prop.getProperty("virtuoso_user"), prop.getProperty("virtuoso_password"));
            Query query = QueryFactory.create("SELECT DISTINCT ?p ?o WHERE { ?s ?p ?o }");
            VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(query, graph);
            ResultSet results = vqe.execSelect();
            while (results.hasNext()) {
                QuerySolution result = results.next();
                RDFNode predicate = result.get("p");
                RDFNode object = result.get("o");
                //System.out.println(String.format("%s - %s", predicate, object));
                if (!exclusionMap.containsKey(predicate.toString())) {
                    Set<String> datasetSet = new HashSet<String>();
                    for (String dataset : datasetList) {
                        if (!dataset.equals(sourceDataset)) {
                            VirtGraph targetGraph = new VirtGraph("http://" + dataset, connectionURL.toString(), prop.getProperty("virtuoso_user"), prop.getProperty("virtuoso_password"));
                            Query predicateQuery = QueryFactory.create(String.format("ASK {?s <%s> ?o}", predicate));
                            VirtuosoQueryExecution vqePredicate = VirtuosoQueryExecutionFactory.create(predicateQuery, targetGraph);
                            boolean ask = vqePredicate.execAsk();
                            if (!ask) {
                                datasetSet.add(dataset);
                            }
                        }
                    }

                    exclusionMap.put(predicate.toString(), datasetSet);
                }
                totalTriples++;
                for (String targetDataset : datasetList) {
                    if ((!doneList.contains(targetDataset)) && (!targetDataset.equals(sourceDataset)) && (!exclusionMap.get(predicate.toString()).contains(targetDataset))) {
                        VirtGraph targetGraph = new VirtGraph("http://" + targetDataset, connectionURL.toString(), prop.getProperty("virtuoso_user"), prop.getProperty("virtuoso_password"));
                        Query targetQuery;
                        if (object.isLiteral()) {
                            targetQuery = QueryFactory.create(String.format("ASK { ?s <%s> \"%s\"}", predicate, object.toString().replace("\"", "").replace("\\", "").replace("\n", "")));
                        } else {
                            targetQuery = QueryFactory.create(String.format("ASK { ?s <%s> <%s>}", predicate, object));
                        }
                        VirtuosoQueryExecution targetVqe = VirtuosoQueryExecutionFactory.create(targetQuery, targetGraph);
                        boolean ask = targetVqe.execAsk();
                        if (ask) {
                            int count = 0;
                            if (map.containsKey(targetDataset)) {
                                count = map.get(targetDataset);
                            }
                            map.put(targetDataset, ++count);
                        }
                        targetGraph.close();
                    }
                }
            }
            totalTripleMap.put(sourceDataset, totalTriples);
            commonMap.put(sourceDataset, map);
            doneList.add(sourceDataset);
            graph.close();
        }

        for (String dataset : datasetList) {
            if (!totalTripleMap.containsKey(dataset)) {
                int tripleCount = 0;
                VirtGraph graph = new VirtGraph("http://" + dataset, connectionURL.toString(), prop.getProperty("virtuoso_user"), prop.getProperty("virtuoso_password"));
                Query query = QueryFactory.create("SELECT DISTINCT ?p ?o WHERE { ?s ?p ?o }");
                VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(query, graph);
                ResultSet results = vqe.execSelect();

                while (results.hasNext()) {
                    tripleCount++;
                }
                totalTripleMap.put(dataset, tripleCount);
            }
        }

        Map<String, Map<String, Float>> scoreMap = new HashMap<>();
        //Calculate score
        Generator<List<String>> datasetCombinations = Itertools.combinations(Itertools.iter(datasetList.iterator()), 2);
        List<String> pair;
        while ((pair = datasetCombinations.next()) != null) {
            int commonTriples = 0;
            if (commonMap.containsKey(pair.get(0))) {
                if (commonMap.get(pair.get(0)).containsKey(pair.get(1))) {
                    commonTriples = commonMap.get(pair.get(0)).get(pair.get(1));
                }
            } else if (commonMap.containsKey(pair.get(1))) {
                if (commonMap.get(pair.get(1)).containsKey(pair.get(0))) {
                    commonTriples = commonMap.get(pair.get(1)).get(pair.get(0));
                }
            }

            int totalTriples = totalTripleMap.get(pair.get(0)) + totalTripleMap.get(pair.get(1));
            float dj = (float) (totalTriples - commonTriples) / totalTriples;
            Map<String, Float> targetScoreMap = new HashMap<>();
            if (scoreMap.containsKey(pair.get(0))) {
                targetScoreMap = scoreMap.get(pair.get(0));
            }

            targetScoreMap.put(pair.get(1), dj);
            scoreMap.put(pair.get(0), targetScoreMap);
        }


        Map<String, List<String>> goldStandard = MatchSubgraphs.loadGoldStandard(true);

        File file = new File("tripleequality.csv");
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter(file));
            String line = "Similarity Threshold;Precision;Recall;F1;Accuracy\n";
            bw.write(line);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //Evaluation
        for (int i = 0; i < 10; i++) {
            int fp = 0;
            int fn = 0;
            int tp = 0;
            int tn = 0;
            double threshold = Double.parseDouble(range[i]);

            for (String sourceDataset : scoreMap.keySet()) {
                for (String targetDataset : scoreMap.get(sourceDataset).keySet()) {
                    float score = scoreMap.get(sourceDataset).get(targetDataset);

                    boolean linked = false;

                    if (goldStandard.containsKey(sourceDataset)) {
                        if (goldStandard.get(sourceDataset).contains(targetDataset)) {
                            linked = true;
                        }
                    } else if (goldStandard.containsKey(targetDataset)) {
                        if (goldStandard.get(targetDataset).contains(sourceDataset)) {
                            linked = true;
                        }
                    }

                    if (score > threshold && linked) {
                        tp++;
                    } else if (score > threshold && !linked) {
                        fp++;
                    } else if (score <= threshold && linked) {
                        fn++;
                    } else if (score <= threshold && !linked) {
                        tn++;
                    }

                }
            }

            System.out.println(String.format("Threshold: %s", threshold));

            double precision = (double) tp / (tp + fp);
            double recall = (double) tp / (tp + fn);
            double f1 = 2 * precision * recall / (precision + recall);
            double accuracy = (double) (tp + tn) / (tp + tn + fp + fn);

            System.out.println(String.format("True positives: %s", tp));
            System.out.println(String.format("False positives: %s", fp));
            System.out.println(String.format("True negatives: %s", tn));
            System.out.println(String.format("False negatives: %s", fn));

            System.out.println(String.format("Precision: %s", precision));
            System.out.println(String.format("Recall: %s", recall));
            System.out.println(String.format("F1: %s", f1));
            System.out.println(String.format("Accuracy: %s", accuracy));

        }

        try {
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        logger.info("End!");
    }
}
