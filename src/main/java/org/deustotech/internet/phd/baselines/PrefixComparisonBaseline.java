package org.deustotech.internet.phd.baselines;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.RDFNode;
import net.ericaro.neoitertools.Generator;
import net.ericaro.neoitertools.Itertools;
import org.apache.http.client.utils.URIBuilder;
import org.deustotech.internet.phd.framework.matchsubgraphs.MatchSubgraphs;
import virtuoso.jena.driver.VirtGraph;
import virtuoso.jena.driver.VirtuosoQueryExecution;
import virtuoso.jena.driver.VirtuosoQueryExecutionFactory;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

/**
 * Created by mikel on 10/06/14.
 */
public class PrefixComparisonBaseline {

    private static String classQuery = "SELECT DISTINCT ?class WHERE { [] a ?class }";
    private static String propertyQuery = "SELECT DISTINCT ?p WHERE { ?s ?p ?o }";

    private static String [] range = new String[] {"0.0", "0.1", "0.2", "0.3", "0.4", "0.5", "0.6", "0.7", "0.8", "0.9"};

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

    public static void run(String csvLocation) {
        BufferedReader br = null;
        Set<String> datasetList = new HashSet<String>();

        Properties prop = new Properties();
        InputStream input = null;
        try {
            input = PrefixComparisonBaseline.class.getResourceAsStream("/config.properties");
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

        try {
            br = new BufferedReader(new FileReader(csvLocation));
            String line;
            while ((line = br.readLine()) != null) {
                String[] sline = line.split(",");
                if (!sline[4].equals(" ") && !sline[4].equals("") && !sline[4].equals("*")) {
                    datasetList.add(sline[4]);
                }
            }
            br.close();

            Map<String, List<String>> prefixMap = new HashMap<String, List<String>>();

            Generator<List<String>> combinations = Itertools.combinations(Itertools.iter(datasetList.iterator()), 2);
            List<String> pair;

            Map<String, Map<String, Float>> matchMap = new HashMap<>();

            try {
                while((pair = combinations.next()) != null) {
                    if (!prefixMap.containsKey(pair.get(0))) {
                        List<String> prefixList = new ArrayList<String>();
                        VirtGraph sourceGraph = new VirtGraph("http://" + pair.get(0), connectionURL.toString(), prop.getProperty("virtuoso_user"), prop.getProperty("virtuoso_password"));
                        Query query = QueryFactory.create(classQuery);
                        VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(query, sourceGraph);
                        ResultSet results = vqe.execSelect();
                        while (results.hasNext()) {
                            QuerySolution result = results.nextSolution();
                            RDFNode res = result.get("class");
                            URI uri = new URI(res.toString());
                            String prefix = getPrefix(uri.toString());
                            if (!prefixList.contains(prefix)) {
                                prefixList.add(prefix);
                            }
                        }

                        query = QueryFactory.create(propertyQuery);
                        vqe = VirtuosoQueryExecutionFactory.create(query, sourceGraph);
                        results = vqe.execSelect();
                        while (results.hasNext()) {
                            QuerySolution result = results.nextSolution();
                            RDFNode res = result.get("p");
                            URI uri = new URI(res.toString());
                            String prefix = getPrefix(uri.toString());
                            if (!prefixList.contains(prefix)) {
                                prefixList.add(prefix);
                            }
                        }

                        prefixMap.put(pair.get(0), prefixList);
                    }

                    if (!prefixMap.containsKey(pair.get(1))) {
                        List<String> prefixList = new ArrayList<String>();
                        VirtGraph sourceGraph = new VirtGraph("http://" + pair.get(1), connectionURL.toString(), prop.getProperty("virtuoso_user"), prop.getProperty("virtuoso_password"));
                        Query query = QueryFactory.create(classQuery);
                        VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(query, sourceGraph);
                        ResultSet results = vqe.execSelect();
                        while (results.hasNext()) {
                            QuerySolution result = results.nextSolution();
                            RDFNode res = result.get("class");
                            URI uri = new URI(res.toString());
                            String prefix = getPrefix(uri.toString());
                            if (!prefixList.contains(prefix)) {
                                prefixList.add(prefix);
                            }
                        }

                        query = QueryFactory.create(propertyQuery);
                        vqe = VirtuosoQueryExecutionFactory.create(query, sourceGraph);
                        results = vqe.execSelect();
                        while (results.hasNext()) {
                            QuerySolution result = results.nextSolution();
                            RDFNode res = result.get("p");
                            URI uri = new URI(res.toString());
                            String prefix = getPrefix(uri.toString());
                            if (!prefixList.contains(prefix)) {
                                prefixList.add(prefix);
                            }
                        }

                        prefixMap.put(pair.get(1), prefixList);
                    }

                    List<String> mergedList = new ArrayList<String>();
                    /*List<String> intersectionList = new ArrayList<String>();

                    for (String item : prefixMap.get(pair.get(0))) {
                        if (!intersectionList.contains(item)) {
                            intersectionList.add(item);
                        }
                    }

                    for (String item : prefixMap.get(pair.get(1))) {
                        if (!intersectionList.contains(item)) {
                            intersectionList.add(item);
                        }
                    }*/
                    mergedList.addAll(prefixMap.get(pair.get(0)));
                    mergedList.addAll(prefixMap.get(pair.get(1)));

                    Generator<List<String>> mergedCombinations = Itertools.combinations(Itertools.iter(mergedList.iterator()), 2);

                    List<String> mergedPair;
                    int total = 0;
                    try {
                        while ((mergedPair = mergedCombinations.next()) != null) {
                            if (mergedPair.get(0).equals(mergedPair.get(1))) {
                                total += 1;
                            }
                        }
                    } catch (NoSuchElementException e1) {
                        // Well, combinations.next() do not return null when the last element is reached.
                    }

                    float similarity = (float) total / Math.max(prefixMap.get(pair.get(0)).size(), prefixMap.get(pair.get(1)).size());
                    System.out.println(String.format("%s - %s (%f)", pair.get(0), pair.get(1), similarity));
                    if (!matchMap.containsKey(pair.get(0))) {
                        matchMap.put(pair.get(0), new HashMap<String, Float>());
                    }
                    Map<String, Float> tempMap = matchMap.get(pair.get(0));
                    tempMap.put(pair.get(1), similarity);
                    matchMap.put(pair.get(0), tempMap);
                }
            } catch (NoSuchElementException e) {
                // Well, combinations.next() do not return null when the last element is reached.
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }

            Map<String, List<String>> goldStandard = MatchSubgraphs.loadGoldStandard(true);

            for (int i = 0; i < 10; i++) {
                int fp = 0;
                int fn = 0;
                int tp = 0;
                int tn = 0;
                double threshold = Double.parseDouble(range[i]);
                for (String source : matchMap.keySet()) {
                    List<String> linkList = goldStandard.get(source);
                    Map<String, Float> scoreMap = matchMap.get(source);
                    for (String target : scoreMap.keySet()) {
                        float score = scoreMap.get(target);
                        if (linkList.contains(target)) {
                            if (score > threshold) {
                                tp++;
                            } else {
                                fn++;
                            }
                        } else {
                            if (score > threshold) {
                                fp++;
                            } else {
                                tn++;
                            }
                        }
                    }
                }
                double precision = (double) tp / (tp + fp);
                double recall = (double) tp / (tp + fn);
                double f1 = 2 * precision * recall / (precision + recall);
                double accuracy = (double) (tp + tn) / (tp + tn + fp + fn);

                System.out.println(String.format("Threshold: %s", threshold));
                System.out.println(String.format("True positives: %s", tp));
                System.out.println(String.format("False positives: %s", fp));
                System.out.println(String.format("True negatives: %s", tn));
                System.out.println(String.format("False negatives: %s", fn));

                System.out.println(String.format("Precision: %s", precision));
                System.out.println(String.format("Recall: %s", recall));
                System.out.println(String.format("F1: %s", f1));
                System.out.println(String.format("Accuracy: %s", accuracy));
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
