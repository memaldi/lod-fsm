package org.deustotech.internet.phd.baselines;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import net.ericaro.neoitertools.Generator;
import net.ericaro.neoitertools.Itertools;
import org.apache.avro.generic.GenericData;
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
    public static void run(String csvLocation) {
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

        Map<String, List<String>> rankingMap = new HashMap<>();

        for (String dataset: datasetList) {
            List<String> ranking = new ArrayList<>();
            Map<String, Float> ontologyStats = datasetPercentStats.get(dataset);
            ValueComparator bvc =  new ValueComparator(ontologyStats);
            TreeMap<String,Double> sortedMap = new TreeMap<>(bvc);

            for (String key : sortedMap.descendingKeySet()) {
                ranking.add(key);
            }

            rankingMap.put(dataset, ranking);
        }

        Generator<List<String>> combinations = Itertools.combinations(Itertools.iter(datasetList.iterator()), 2);
        List<String> pair;

        try {
            while((pair = combinations.next()) != null) {
                int K = 0;
                List<String> sourceRanking;
                List<String> targetRanking;
                if (rankingMap.get(pair.get(0)).size() >= rankingMap.get(pair.get(1)).size() ) {
                    sourceRanking = new ArrayList<>(rankingMap.get(pair.get(0)));
                    targetRanking = new ArrayList<>(rankingMap.get(pair.get(1)));
                } else {
                    sourceRanking = new ArrayList<>(rankingMap.get(pair.get(1)));
                    targetRanking = new ArrayList<>(rankingMap.get(pair.get(0)));
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
                System.out.println(KN);

            }
        } catch (NoSuchElementException e) {

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
