package org.deustotech.internet.phd.baselines;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import net.ericaro.neoitertools.Generator;
import net.ericaro.neoitertools.Itertools;
import org.apache.avro.generic.GenericData;
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
 * Created by memaldi on 12/12/14.
 */
public class OntologyRankingBaseline {

    private static String [] range = new String[] {"0.0", "0.1", "0.2", "0.3", "0.4", "0.5", "0.6", "0.7", "0.8", "0.9"};

    public static void run() {
        Logger logger = Logger.getLogger(OntologyRankingBaseline.class.getName());
        logger.info("Initializing...");
        Properties prop = new Properties();
        InputStream input;

        try {
            input = OntologyRankingBaseline.class.getResourceAsStream("/config.properties");
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




        Map<String, Map<String, Float>> datasetStats = new HashMap<>();

        for (String dataset : datasetList) {
            logger.info(String.format("Analyzing %s...", dataset));
            Map<String, Float> ontologyStats = new HashMap<>();
            VirtGraph graph = new VirtGraph("http://" + dataset, connectionURL.toString(), prop.getProperty("virtuoso_user"), prop.getProperty("virtuoso_password"));
            Query query = QueryFactory.create("select ?Class where {[] a ?Class}");
            VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(query, graph);
            ResultSet results = vqe.execSelect();
            while (results.hasNext()) {
                QuerySolution result = results.next();
                String clazz = result.get("Class").toString();

                String ontologyURI = getPrefix(clazz);
                float accum = 0;
                if (ontologyStats.containsKey(ontologyURI)) {
                    accum = ontologyStats.get(ontologyURI);
                }
                accum++;
                ontologyStats.put(getPrefix(clazz), accum);
            }

            query = QueryFactory.create("select ?property where {?s ?property ?o}");
            vqe = VirtuosoQueryExecutionFactory.create(query, graph);
            results = vqe.execSelect();
            while (results.hasNext()) {
                QuerySolution result = results.next();
                String property = result.get("property").toString();

                String ontologyURI = getPrefix(property);
                float accum = 0;
                if (ontologyStats.containsKey(ontologyURI)) {
                    accum = ontologyStats.get(ontologyURI);
                }
                accum++;
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

        Map<String, List<String>> rankingMap = new HashMap<>();

        for (String dataset: datasetList) {
            List<String> ranking = new ArrayList<>();
            Map<String, Float> ontologyStats = datasetPercentStats.get(dataset);
            ValueComparator bvc =  new ValueComparator(ontologyStats);
            TreeMap<String, Float> sortedMap = new TreeMap<>(bvc);

            sortedMap.putAll(ontologyStats);

            for (String key : sortedMap.keySet()) {
                ranking.add(key);
            }

            rankingMap.put(dataset, ranking);
        }

        Generator<List<String>> combinations = Itertools.combinations(Itertools.iter(datasetList.iterator()), 2);
        List<String> pair;

        Map<String, Map<String, Float>> distanceMap = new HashMap<>();

        try {
            while((pair = combinations.next()) != null) {
                int K = 0;

                List<String> sourceRanking = new ArrayList<>(rankingMap.get(pair.get(0)));
                List<String> targetRanking = new ArrayList<>(rankingMap.get(pair.get(1)));

                for (String item : sourceRanking) {
                    if (!targetRanking.contains(item)) {
                        targetRanking.add(item);
                    }
                }

                for (String item: targetRanking) {
                    if(!sourceRanking.contains(item)) {
                        sourceRanking.add(item);
                    }
                }

                Generator<List<String>> itemCombinations = Itertools.combinations(Itertools.iter(sourceRanking.iterator()), 2);
                List<String> itemPair;
                try {
                    while ((itemPair = itemCombinations.next()) != null) {
                        if ((sourceRanking.indexOf(itemPair.get(0)) > sourceRanking.indexOf(itemPair.get(1)) && targetRanking.indexOf(itemPair.get(0)) < targetRanking.indexOf(itemPair.get(1))) || (sourceRanking.indexOf(itemPair.get(0)) < sourceRanking.indexOf(itemPair.get(1)) && targetRanking.indexOf(itemPair.get(0)) > targetRanking.indexOf(itemPair.get(1)))) {
                            K++;
                        }

                    }
                } catch (NoSuchElementException e) {

                }

                float KN = (float) K / (sourceRanking.size() * (sourceRanking.size() - 1) / 2);
                System.out.println(String.format("%s - %s (%s)", pair.get(0), pair.get(1), KN));

                Map<String, Float> targetDistanceMap = new HashMap<>();
                if (distanceMap.containsKey(pair.get(0))) {
                    targetDistanceMap = distanceMap.get(pair.get(0));
                }

                targetDistanceMap.put(pair.get(1), KN);
                distanceMap.put(pair.get(0), targetDistanceMap);

            }
        } catch (NoSuchElementException e) {

        }

        Map<String, List<String>> goldStandard = MatchSubgraphs.loadGoldStandard(true);

        for (int j = 0; j < 10; j += 1 ) {
            int tp = 0;
            int fp = 0;
            int tn = 0;
            int fn = 0;
            double threshold = Double.parseDouble(range[j]);
            for (String sourceDataset : distanceMap.keySet()) {
                for (String targetDataset : distanceMap.get(sourceDataset).keySet()) {
                    float score = 0;
                    if (distanceMap.containsKey(sourceDataset)) {
                        if (distanceMap.get(sourceDataset).containsKey(targetDataset)) {
                            score = distanceMap.get(sourceDataset).get(targetDataset);
                        }
                    } else if (distanceMap.containsKey(targetDataset)) {
                        if (distanceMap.get(targetDataset).containsKey(sourceDataset)) {
                            score = distanceMap.get(targetDataset).get(sourceDataset);
                        }
                    }

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

class ValueComparator implements Comparator<String> {

    Map<String, Float> base;
    public ValueComparator(Map<String, Float> base) {
        this.base = base;
    }

    @Override
    public int compare(String a, String b) {
        if (base.get(a) >= base.get(b)) {
            return -1;
        } else {
            return 1;
        } // returning 0 would merge keys
    }
}
