package org.deustotech.internet.phd.framework.matchsubgraphs;

import net.ericaro.neoitertools.Generator;
import net.ericaro.neoitertools.Itertools;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.deustotech.internet.phd.framework.model.Dataset;
import org.deustotech.internet.phd.framework.model.Edge;
import org.deustotech.internet.phd.framework.model.Graph;
import org.deustotech.internet.phd.framework.model.Vertex;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.util.*;

/**
 * Created by mikel (m.emaldi at deusto dot es) on 20/06/14.
 */
public class MatchSubgraphs {
    public static void run(double similarityThreshold, String subduePath, boolean applyStringDistances, String surveyDatasetsLocation) {
        Configuration conf = HBaseConfiguration.create();
        HTable htable = null;
        try {
            htable = new HTable(conf, "subgraphs");
        } catch (IOException e) {
            e.printStackTrace();
        }
        List<Filter> filterList = new ArrayList<>();
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("type"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes("vertex"));
        filterList.add(filter);
        FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL, filterList);
        Scan scan = new Scan();
        scan.setFilter(fl);

        Set<String> graphSet = new HashSet<>();

        try {
            ResultScanner scanner = htable.getScanner(scan);
            Result result;
            while ((result = scanner.next()) != null) {
                String graph = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("graph")));
                graphSet.add(graph);
            }
            scanner.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Debug
        graphSet = new HashSet<>();
        graphSet.add("hedatuz.g");
        graphSet.add("risk.g");
        // Debug end

        Map<String, Map<String, Double>> similarityMap = new HashMap<>();

        Generator<List<String>> graphPermutations = Itertools.permutations(Itertools.iter(graphSet.iterator()), 2);
        boolean end = false;
        while(!end) {
            try {
                List<String> pair = graphPermutations.next();
                if (!pair.get(0).equals(pair.get(1))) {
                    Graph sourceGraph = getGraph(pair.get(0), htable);
                    Graph targetGraph = getGraph(pair.get(1), htable);
                    List<Graph> matchedGraphs = matchGraphs(sourceGraph, targetGraph, conf, applyStringDistances, similarityThreshold);
                    Graph sourceMatchedGraph = matchedGraphs.get(0);
                    Graph targetMatchedGraph = matchedGraphs.get(1);

                    double distance = getDistance(sourceMatchedGraph, targetMatchedGraph, subduePath);
                    double maxLength = Math.max(sourceMatchedGraph.getVertices().size() + sourceMatchedGraph.getEdges().size(), targetMatchedGraph.getVertices().size() + targetMatchedGraph.getEdges().size());
                    // TODO: check this!
                    if (distance > maxLength) {
                        maxLength = distance;
                    }
                    double absoluteDistance =  distance / maxLength;
                    double similarity = 1 - absoluteDistance;

                    System.out.println(String.format("%s - %s (%f)", sourceGraph.getName(), targetGraph.getName(), similarity));

                    if (!similarityMap.containsKey(sourceGraph.getName())) {
                        similarityMap.put(sourceGraph.getName(), new HashMap<String, Double>());
                    }
                    Map<String, Double> map = similarityMap.get(sourceGraph.getName());
                    map.put(targetGraph.getName(), similarity);
                    similarityMap.put(sourceGraph.getName(), map);

                }
            } catch (NoSuchElementException e) {
                end = true;
            }
        }
        Map<Integer, Dataset> datasets = getDatasets(surveyDatasetsLocation);
        Map<Integer, Map<Integer, String>> goldStandard = loadGoldStandard(surveyDatasetsLocation);
        Map<String, Integer> name2keyMap = getKeyFromName(datasets);

        int tp = 0;
        int fp = 0;
        int tn = 0;
        int fn = 0;

        for (double i = 0; i < 1; i += 0.1 ) {
            for (String source : similarityMap.keySet()) {
                int sourceKey = name2keyMap.get(source);
                for (String target : similarityMap.keySet()) {
                    if (!source.equals(target)) {
                        int targetKey = name2keyMap.get(target);
                        String value = goldStandard.get(sourceKey).get(targetKey);
                        Double similarity = similarityMap.get(source).get(target);
                        if (similarity > i && value.equals("yes")) {
                            tp++;
                        } else if (similarity > i && value.equals("no")) {
                            fp++;
                        } else if (similarity < i && value.equals("yes")) {
                            fn++;
                        } else if (similarity < i && value.equals("no")) {
                            tn++;
                        }
                    }
                }
            }
        }
        System.out.println("");

        try {
            htable.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static Map<String, Integer> getKeyFromName(Map<Integer, Dataset> datasets) {
        Map<String, Integer> key2name = new HashMap<>();
        for (int key : datasets.keySet()) {
            key2name.put(datasets.get(key).getName(), key);
        }

        return key2name;
    }

    private static Map<Integer, Dataset>  getDatasets(String surveyDatasetsLocation) {
        Map<Integer, Dataset> datasetMap = new HashMap<>();
        File jsonFile = new File("/home/mikel/doctorado/src/java/baselines/all.json");
        Map<String, String> URL2NameMap = URL2Name(surveyDatasetsLocation);
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(jsonFile));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        String jsonString = "";
        String line;
        try {
            while((line = br.readLine()) != null) {
                jsonString += line;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        JSONArray jsonArray = new JSONArray(jsonString);
        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject jsonObject = jsonArray.getJSONObject(i);
            String model = jsonObject.getString("model");
            switch (model) {
                case "survey.dataset":
                    // Search dataset name in CSV
                    String name = URL2NameMap.get(jsonObject.getJSONObject("fields").getString("datahub_url")) + ".g";
                    Dataset dataset = new Dataset(jsonObject.getJSONObject("fields").getString("datahub_url"), name, jsonObject.getInt("pk"));
                    datasetMap.put((Integer) jsonObject.get("pk"), dataset);
                    break;
                default:
                    break;
            }
        }
        return datasetMap;
    }

    private static Map<Integer, Map<Integer, String>> loadGoldStandard(String surveyDatasetsLocation) {
        // Load gold standard
        Map<Integer, Map<Integer, Map<String, Integer>>> ratingMap = new HashMap<>();
        File jsonFile = new File("/home/mikel/doctorado/src/java/baselines/all.json");
        try {
            BufferedReader br = new BufferedReader(new FileReader(jsonFile));
            String jsonString = "";
            String line;
            while((line = br.readLine()) != null) {
                jsonString += line;
            }
            JSONArray jsonArray = new JSONArray(jsonString);
            for (int i = 0; i < jsonArray.length(); i++) {
                JSONObject jsonObject = jsonArray.getJSONObject(i);
                String model = jsonObject.getString("model");
                switch (model) {
                    case "survey.similarity":
                        int sourceDataset = jsonObject.getJSONObject("fields").getInt("source_dataset");
                        int targetDataset = jsonObject.getJSONObject("fields").getInt("target_dataset");
                        String similarity = jsonObject.getJSONObject("fields").getString("similarity");
                        if (!ratingMap.containsKey(sourceDataset)) {
                            ratingMap.put(sourceDataset, new HashMap<Integer, Map<String, Integer>>());
                        }
                        Map<Integer, Map<String, Integer>> targetMap = ratingMap.get(sourceDataset);

                        if (!targetMap.containsKey(targetDataset)) {
                            Map<String, Integer> pairRatingMap = new HashMap<>();
                            pairRatingMap.put("yes", 0);
                            pairRatingMap.put("no", 0);
                            pairRatingMap.put("undefined", 0);
                            targetMap.put(targetDataset, pairRatingMap);
                        }
                        Map<String, Integer> pairRatingMap = targetMap.get(targetDataset);
                        pairRatingMap.put(similarity, pairRatingMap.get(similarity) + 1);

                        targetMap.put(targetDataset, pairRatingMap);
                        ratingMap.put(sourceDataset, targetMap);
                    default:
                        break;
                }
            }
            Map<Integer, Map<Integer, String>> filteredRatingMap = new HashMap<>();
            for (int sourceDataset : ratingMap.keySet()) {
                Map<Integer, Map<String, Integer>> targetMap = ratingMap.get(sourceDataset);
                for (int targetDataset : targetMap.keySet()) {
                    Map<String, Integer> pairRatingMap = targetMap.get(targetDataset);
                    int total = 0;
                    int maxRatingValue = 0;
                    String maxRating = null;
                    for (String rating : pairRatingMap.keySet()) {
                        total += pairRatingMap.get(rating);
                        if (pairRatingMap.get(rating) > maxRatingValue) {
                            maxRatingValue = pairRatingMap.get(rating);
                            maxRating = rating;
                        }
                    }
                    if (total >= 3 && maxRatingValue >= 2) {
                        if (!filteredRatingMap.containsKey(sourceDataset)) {
                            filteredRatingMap.put(sourceDataset, new HashMap<Integer, String>());
                        }
                        Map<Integer, String> filteredTargetMap = filteredRatingMap.get(sourceDataset);
                        filteredTargetMap.put(targetDataset, maxRating);
                    }
                }
            }
            return filteredRatingMap;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static Map<String, String> URL2Name(String surveyDatasetsLocation) {
        File surveyDatasets = new File(surveyDatasetsLocation);
        Map<String, String> URL2NameMap = new HashMap<>();
        try {
            BufferedReader br = new BufferedReader(new FileReader(surveyDatasets));
            boolean header = true;
            String line;
            while((line = br.readLine()) != null) {
                if (header) {
                    header = false;
                } else {
                    if (line.split(",").length >= 5) {
                        String URL = line.split(",")[1];
                        String name = line.split(",")[4];
                        URL2NameMap.put(URL, name);
                    }
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return URL2NameMap;
    }

    private static double getDistance(Graph sourceMatchedGraph, Graph targetMatchedGraph, String subduePath) {
        String sourcePath = writeGraph(sourceMatchedGraph);
        String targetPath = writeGraph(targetMatchedGraph);
        return callSubdue(sourcePath, targetPath, subduePath);
    }

    private static double callSubdue(String sourcePath, String targetPath, String subduePath) {
        try {
            Process process = new ProcessBuilder(String.format("%s/bin/gm", subduePath), sourcePath, targetPath).start();
            InputStream is = process.getInputStream();
            //InputStream is = process.getErrorStream();
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);
            String line;
            double value = 0;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("Match Cost")) {
                    value = Double.parseDouble(line.split(" ")[3]);
                }
            }

            return value;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return -1;
    }

    private static String writeGraph(Graph graph) {
        try {
            String fileName = UUID.randomUUID().toString();
            File file = File.createTempFile(fileName, ".g");
            file.deleteOnExit();
            BufferedWriter bw = new BufferedWriter(new FileWriter(file));
            TreeMap<Long, Vertex> vertexTreeMap = new TreeMap<>();
            for (Vertex vertex : graph.getVertices()) {
                vertexTreeMap.put(vertex.getId(), vertex);
            }
            for (long id : vertexTreeMap.keySet()) {
                bw.write(String.format("v %s %s\n", vertexTreeMap.get(id).getId(), vertexTreeMap.get(id).getLabel()));
            }
            bw.flush();

            for (Vertex vertex: graph.getVertices()) {
                for (Edge edge : vertex.getEdges()) {
                    bw.write(String.format("d %s %s %s\n", vertex.getId(), edge.getTarget().getId(), edge.getLabel()));
                }
            }
            bw.close();

            return file.getAbsolutePath();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static List<Graph> matchGraphs(Graph sourceGraph, Graph targetGraph, Configuration conf, boolean applyStringDistances, double similarityThreshold) {
        Graph matchedSourceGraph;
        Graph matchedTargetGraph;
        if (applyStringDistances) {
            HTable table = null;
            try {
                table = new HTable(conf, "alignments");
            } catch (IOException e) {
                e.printStackTrace();
            }

            Set<String> labelSet = new HashSet<>();
            Set<String> edgeSet = new HashSet<>();
            for (Vertex vertex : sourceGraph.getVertices()) {
                labelSet.add(vertex.getLabel());
                for (Edge edge : vertex.getEdges()) {
                    edgeSet.add(edge.getLabel());
                }
            }
            for (Vertex vertex : targetGraph.getVertices()) {
                labelSet.add(vertex.getLabel());
                for (Edge edge : vertex.getEdges()) {
                    edgeSet.add(edge.getLabel());
                }
            }

            Map<String, Map<String, Double>> distanceMap = new HashMap<>();
            Generator<List<String>> vertexPermutations;
            if (labelSet.size() >= 2) {
                vertexPermutations = Itertools.permutations(Itertools.iter(labelSet.iterator()), 2);
                distanceMap.putAll(getDistance(table, vertexPermutations));
            }
            Generator<List<String>> edgePermutations;
            if (edgeSet.size() >= 2) {
                edgePermutations = Itertools.permutations(Itertools.iter(edgeSet.iterator()), 2);
                distanceMap.putAll(getDistance(table, edgePermutations));
            }

            Map<String, String> replaceMap = new HashMap<>();

            for (String label : distanceMap.keySet()) {
                Map<String, Double> map = distanceMap.get(label);
                double minDistance = 2;
                String minLabel = "";
                for (String key : map.keySet()) {
                    if (map.get(key) < minDistance) {
                        minDistance = map.get(key);
                        minLabel = key;
                    }
                }
                if (1 - minDistance > similarityThreshold) {
                    String uuid = UUID.randomUUID().toString();
                    replaceMap.put(label, uuid);
                    replaceMap.put(minLabel, uuid);
                }
            }
            matchedSourceGraph = getMatchedGraph(sourceGraph, replaceMap);
            matchedTargetGraph = getMatchedGraph(targetGraph, replaceMap);

            List<Graph> result = new ArrayList<>();
            result.add(matchedSourceGraph);
            result.add(matchedTargetGraph);

            return result;
        } else {
            List<Graph> result = new ArrayList<>();
            result.add(sourceGraph);
            result.add(targetGraph);

            return  result;
        }


    }

    private static Graph getMatchedGraph(Graph graph, Map<String, String> replaceMap) {
        Graph matchedGraph = new Graph(graph.getName());
        for (Vertex vertex : graph.getVertices()) {
            long id = vertex.getId();
            String label = vertex.getLabel();
            if (replaceMap.containsKey(label)) {
                label = replaceMap.get(label);
            }
            Vertex newVertex = new Vertex(label, id);
            matchedGraph.addVertex(newVertex);
        }

        for (Vertex vertex : graph.getVertices()) {
            for (Edge edge : vertex.getEdges()) {
                Vertex matchedSourceVertex = matchedGraph.getVertex(vertex.getId());
                Vertex matchedTargetVertex = matchedGraph.getVertex(edge.getTarget().getId());
                String label = edge.getLabel();
                if (replaceMap.containsKey(label)) {
                    label = replaceMap.get(label);
                }
                Edge matchedEdge = new Edge(label, matchedTargetVertex);
                matchedSourceVertex.addEdge(matchedEdge);
                matchedGraph.updateVertex(matchedSourceVertex);
            }
        }
        return matchedGraph;
    }

    private static Map<String, Map<String, Double>> getDistance(HTable table, Generator<List<String>> vertexPermutations) {
        Map<String, Map<String, Double>> distanceMap = new HashMap<>();
        boolean end = false;
        while(!end) {
            try {
                List<String> pair = vertexPermutations.next();
                List<Filter> filterList = new ArrayList<>();
                SingleColumnValueFilter sourceFilter = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("source"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(pair.get(0)));
                SingleColumnValueFilter targetFilter = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("target"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(pair.get(1)));
                SingleColumnValueFilter meanFilter = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("distance"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes("geometricMean"));
                filterList.add(sourceFilter);
                filterList.add(targetFilter);
                filterList.add(meanFilter);
                FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL, filterList);
                Scan scan = new Scan();
                scan.setFilter(fl);

                try {
                    ResultScanner scanner = table.getScanner(scan);
                    Result result;
                    while((result = scanner.next()) != null) {
                        double value = Bytes.toDouble(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("value")));
                        if (!distanceMap.containsKey(pair.get(0))) {
                            distanceMap.put(pair.get(0), new HashMap<String, Double>());
                        }
                        Map<String, Double> map = distanceMap.get(pair.get(0));
                        map.put(pair.get(1), value);
                        distanceMap.put(pair.get(0), map);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } catch (NoSuchElementException e) {
                end = true;
            }
        }
        return distanceMap;
    }

    private static Graph getGraph(String graphName, HTable table) {
        // Get vertices
        List<Filter> filterList = new ArrayList<>();
        SingleColumnValueFilter graphFilter = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("graph"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(graphName));
        SingleColumnValueFilter vertexFilter = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("type"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes("vertex"));
        filterList.add(graphFilter);
        filterList.add(vertexFilter);
        FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL, filterList);
        Scan scan = new Scan();
        scan.setFilter(fl);
        Graph graph = new Graph(graphName);
        try {
            ResultScanner scanner = table.getScanner(scan);
            Result result;
            while((result = scanner.next()) != null) {
                long id = Bytes.toLong(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("id")));
                String label = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("label")));
                Vertex vertex = new Vertex(label, id);
                graph.addVertex(vertex);
            }
            scanner.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        // Get edges
        filterList = new ArrayList<>();
        graphFilter = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("graph"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(graphName));
        vertexFilter = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("type"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes("edge"));
        filterList.add(graphFilter);
        filterList.add(vertexFilter);
        fl = new FilterList(FilterList.Operator.MUST_PASS_ALL, filterList);
        scan = new Scan();
        scan.setFilter(fl);
        try {
            ResultScanner scanner = table.getScanner(scan);
            Result result;
            while((result = scanner.next()) != null) {
                long source = Bytes.toLong(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("source")));
                long target = Bytes.toLong(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("target")));
                String label = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("label")));
                Vertex sourceVertex = graph.getVertex(source);
                Vertex targetVertex = graph.getVertex(target);
                Edge edge = new Edge(label, targetVertex);
                sourceVertex.addEdge(edge);
                graph.updateVertex(sourceVertex);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return graph;
    }
}
