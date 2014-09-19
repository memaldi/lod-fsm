package org.deustotech.internet.phd.framework.matchsubgraphs;

import net.ericaro.neoitertools.Generator;
import net.ericaro.neoitertools.Itertools;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.thrift.TException;
import org.deustotech.internet.phd.framework.model.Dataset;
import org.deustotech.internet.phd.framework.model.Edge;
import org.deustotech.internet.phd.framework.model.Graph;
import org.deustotech.internet.phd.framework.model.Vertex;
import org.hypertable.thrift.ThriftClient;
import org.hypertable.thriftgen.*;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.*;

/**
 * Created by mikel (m.emaldi at deusto dot es) on 20/06/14.
 */
public class MatchSubgraphs {

    private static String [] range = new String[] {"0.0", "0.1", "0.2", "0.3", "0.4", "0.5", "0.6", "0.7", "0.8", "0.9"};

    public static void run(double similarityThreshold, String subduePath, boolean applyStringDistances, String outputFile, int deep) {
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

        Set<String> graphSet = new HashSet<>();

        try {
            String query = "SELECT * from subgraphs where type = 'vertex'";

            HqlResult hqlResult = client.hql_query(ns, query);

            if (hqlResult.getCells().size() > 0) {
                for (Cell cell : hqlResult.getCells()) {
                    ByteBuffer graphBuffer = client.get_cell(ns, "subgraphs", cell.getKey().getRow(), "graph");
                    String graph = new String(graphBuffer.array(), graphBuffer.position(), graphBuffer.remaining());
                    graphSet.add(graph);
                }
            }
        } catch (ClientException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }




        File file = new File(outputFile);
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter(file));
            String line = "Ontology Matching Threshold;Similarity Threshold;Precision;Recall;F1;Accuracy\n";
            bw.write(line);
        } catch (IOException e) {
            e.printStackTrace();
        }

        createSimilarityTable(client, ns);
        for (int j = 0; j < 10; j += 1 ) {
            double sim = Double.parseDouble(range[j]);
            boolean end = false;
            Generator<List<String>> graphPermutations = Itertools.combinations(Itertools.iter(graphSet.iterator()), 2);
            String query = String.format("SELECT * FROM similarity WHERE threshold = '%s' LIMIT 1", sim);
            int resultSize = 0;
            try {
                HqlResult hqlResult = client.hql_query(ns, query);
                resultSize = hqlResult.getCells().size();
            } catch (TException e) {
                e.printStackTrace();
            }

            List<Cell> cells = new ArrayList<>();

            if (resultSize <= 0) {

                while (!end) {
                    try {
                        List<String> pair = graphPermutations.next();
                        if (!pair.get(0).equals(pair.get(1))) {
                            Graph sourceGraph = getGraph(pair.get(0), client, ns);
                            Graph targetGraph = getGraph(pair.get(1), client, ns);
                            List<Graph> matchedGraphs = matchGraphs(sourceGraph, targetGraph, client, ns, applyStringDistances, sim);
                            Graph sourceMatchedGraph = matchedGraphs.get(0);
                            Graph targetMatchedGraph = matchedGraphs.get(1);

                            double distance = getDistance(sourceMatchedGraph, targetMatchedGraph, subduePath);
                            double maxLength = Math.max(sourceMatchedGraph.getVertices().size() + sourceMatchedGraph.getEdges().size(), targetMatchedGraph.getVertices().size() + targetMatchedGraph.getEdges().size());
                            // TODO: check this!
                            if (distance > maxLength) {
                                maxLength = distance;
                            }
                            double absoluteDistance = distance / maxLength;
                            double similarity = 1 - absoluteDistance;

                            String keyID = UUID.randomUUID().toString();
                            Key key = new Key();
                            key.setRow(keyID);
                            key.setColumn_family("source");
                            Cell cell = new Cell();
                            cell.setKey(key);

                            try {
                                cell.setValue(sourceGraph.getName().getBytes("UTF-8"));
                            } catch (UnsupportedEncodingException e) {
                                e.printStackTrace();
                            }

                            cells.add(cell);

                            key = new Key();
                            key.setRow(keyID);
                            key.setColumn_family("target");
                            cell = new Cell();
                            cell.setKey(key);

                            try {
                                cell.setValue(targetGraph.getName().getBytes("UTF-8"));
                            } catch (UnsupportedEncodingException e) {
                                e.printStackTrace();
                            }

                            cells.add(cell);

                            key = new Key();
                            key.setRow(keyID);
                            key.setColumn_family("threshold");
                            cell = new Cell();
                            cell.setKey(key);

                            try {
                                cell.setValue(String.valueOf(sim).getBytes("UTF-8"));
                            } catch (UnsupportedEncodingException e) {
                                e.printStackTrace();
                            }

                            cells.add(cell);

                            key = new Key();
                            key.setRow(keyID);
                            key.setColumn_family("value");
                            cell = new Cell();
                            cell.setKey(key);

                            try {
                                cell.setValue(String.valueOf(similarity).getBytes("UTF-8"));
                            } catch (UnsupportedEncodingException e) {
                                e.printStackTrace();
                            }

                            cells.add(cell);

                        }
                    } catch (NoSuchElementException e) {
                        end = true;
                        try {
                            client.set_cells(ns, "similarity", cells);
                        } catch (TException e1) {
                            e1.printStackTrace();
                        }

                    }
                }
            }
            Map<String, List<String>> goldStandard = loadGoldStandard();

            for (int k = 0; k < 10; k += 1) {
                double i = Double.parseDouble(range[k]);
                graphPermutations = Itertools.combinations(Itertools.iter(graphSet.iterator()), 2);
                end = false;
                int tp = 0;
                int fp = 0;
                int tn = 0;
                int fn = 0;
                while (!end) {
                    try {
                        List<String> pair = graphPermutations.next();
                        String source = pair.get(0);
                        String target = pair.get(1);
                        List<String> linkList = goldStandard.get(source.replace(".g", "").toLowerCase());
                        if (!source.equals(target)) {
                            String value = "no";
                            if (linkList != null) {
                                if (linkList.contains(target.replace(".g", "").toLowerCase())) {
                                    value = "yes";
                                } else if (deep > 0) {
                                    List<String> targetLinkList = new ArrayList<String>(goldStandard.get(target.replace(".g", "").toLowerCase()));
                                    targetLinkList.retainAll(linkList);
                                    if ((double) targetLinkList.size() / Math.max(linkList.size(), targetLinkList.size()) > 0.5) {
                                        value = "yes";
                                    }
                                }
                            }

                            Double similarity = null;

                            query = String.format("SELECT * FROM similarity WHERE source = '%s'", source);
                            HqlResult hqlResult = client.hql_query(ns, query);
                            if (hqlResult.getCells().size() > 0) {
                                for (Cell cell : hqlResult.getCells()) {
                                    ByteBuffer targetBuffer = client.get_cell(ns, "similarity", cell.getKey().getRow(), "target");
                                    String stringTarget = new String(targetBuffer.array(), targetBuffer.position(), targetBuffer.remaining());

                                    if (stringTarget.equals(target)) {
                                        ByteBuffer thresholdBuffer = client.get_cell(ns, "similarity", cell.getKey().getRow(), "threshold");
                                        String stringThreshold = new String(thresholdBuffer.array(), thresholdBuffer.position(), thresholdBuffer.remaining());

                                        if (Double.valueOf(stringThreshold) == sim) {

                                            ByteBuffer valueBuffer = client.get_cell(ns, "similarity", cell.getKey().getRow(), "value");
                                            String stringValue = new String(valueBuffer.array(), valueBuffer.position(), valueBuffer.remaining());
                                            similarity = Double.valueOf(stringValue);
                                            break;
                                        }
                                    }
                                }
                            }
                            if (similarity == null) {
                                query = String.format("SELECT * FROM similarity WHERE source = '%s'", source);
                                hqlResult = client.hql_query(ns, query);
                                if (hqlResult.getCells().size() > 0) {
                                    for (Cell cell : hqlResult.getCells()) {
                                        ByteBuffer targetBuffer = client.get_cell(ns, "similarity", cell.getKey().getRow(), "target");
                                        String stringTarget = new String(targetBuffer.array(), targetBuffer.position(), targetBuffer.remaining());

                                        if (stringTarget.equals(target)) {
                                            ByteBuffer thresholdBuffer = client.get_cell(ns, "similarity", cell.getKey().getRow(), "threshold");
                                            String stringThreshold = new String(thresholdBuffer.array(), thresholdBuffer.position(), thresholdBuffer.remaining());

                                            if (Double.valueOf(stringThreshold) == sim) {

                                                ByteBuffer valueBuffer = client.get_cell(ns, "similarity", cell.getKey().getRow(), "value");
                                                String stringValue = new String(valueBuffer.array(), valueBuffer.position(), valueBuffer.remaining());
                                                similarity = Double.valueOf(stringValue);
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                            if (similarity > i && value.equals("yes")) {
                                tp++;
                            } else if (similarity > i && value.equals("no")) {
                                fp++;
                            } else if (similarity <= i && value.equals("yes")) {
                                fn++;
                            } else if (similarity <= i && value.equals("no")) {
                                tn++;
                            }
                        }
                    } catch (NoSuchElementException e) {
                        end = true;
                    } catch (ClientException e) {
                        e.printStackTrace();
                    } catch (TException e) {
                        e.printStackTrace();
                    }
                }
                System.out.println(String.format("Ontology Matching Threshold: %s", sim));
                System.out.println(String.format("Threshold: %s", i));

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

                String line = String.format("%s;%s;%s;%s;%s;%s\n", sim, i, precision, recall, f1, accuracy);
                try {
                    bw.write(line);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        try {
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static void createSimilarityTable(ThriftClient client, long ns) {
        Map columnFamilies = new HashMap();
        Schema schema = new Schema();

        ColumnFamilySpec cf = new ColumnFamilySpec();
        cf.setName("source");
        columnFamilies.put("source", cf);

        cf = new ColumnFamilySpec();
        cf.setName("target");
        columnFamilies.put("target", cf);

        cf = new ColumnFamilySpec();
        cf.setName("threshold");
        columnFamilies.put("threshold", cf);

        cf = new ColumnFamilySpec();
        cf.setName("value");
        columnFamilies.put("value", cf);

        schema.setColumn_families(columnFamilies);

        try {
            client.table_create(ns, "similarity", schema);
        } catch (TException e) {
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
        //File jsonFile = new File("/home/mikel/doctorado/src/java/baselines/all.json");
        Map<String, String> URL2NameMap = URL2Name(surveyDatasetsLocation);
        BufferedReader br = null;
        /*try {
            br = new BufferedReader(new FileReader(jsonFile));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }*/
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

    private static Map<String, List<String>> loadGoldStandard() {

        ThriftClient client = null;
        try {
            client = ThriftClient.create("localhost", 15867);
        } catch (TException e) {
            System.exit(1);
        }

        long ns = 0;
        try {
            if (!client.namespace_exists("gs")) {
                client.namespace_create("gs");
            }
            ns = client.namespace_open("gs");
        } catch (TException e) {
            e.printStackTrace();
        }

        String query = "SELECT * from datahubgs";

        HqlResult hqlResult = null;
        try {
            hqlResult = client.hql_query(ns, query);
        } catch (TException e) {
            e.printStackTrace();
        }

        Map<String, List<String>> datahubGS = new HashMap<>();

        for (Cell cell : hqlResult.getCells()) {
            if (cell.getKey().getColumn_family().equals("links")) {

                String nickname = null;
                try {
                    ByteBuffer nickBuffer = client.get_cell(ns, "datahubgs", cell.getKey().getRow(), "nickname");
                    nickname = new String(nickBuffer.array(), nickBuffer.position(), nickBuffer.remaining());

                } catch (TException e) {
                    e.printStackTrace();
                }

                String stringLinks = Bytes.toString(cell.getValue());
                String[] sline = new String[0];
                if (stringLinks != null) {
                    sline = stringLinks.split(",");
                }
                List<String> linkList = new ArrayList<>();
                for (int i = 0; i < sline.length; i++) {
                    if (!sline[i].equals("")) {
                        try {
                            ByteBuffer linkBuffer = client.get_cell(ns, "datahubgs", sline[i], "nickname");
                            String link = new String(linkBuffer.array(), linkBuffer.position(), linkBuffer.remaining());
                            if (!link.equals("")) {
                                linkList.add(link);
                            }
                        } catch (TException e) {
                            e.printStackTrace();
                        }
                    }
                }
                datahubGS.put(nickname.toLowerCase(), linkList);
            }
        }

        return datahubGS;

        // Load gold standard
        /*Map<Integer, Map<Integer, Map<String, Integer>>> ratingMap = new HashMap<>();
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
        return null;*/
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
                    bw.write(String.format("u %s %s %s\n", vertex.getId(), edge.getTarget().getId(), edge.getLabel()));
                }
            }
            bw.close();

            return file.getAbsolutePath();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static List<Graph> matchGraphs(Graph sourceGraph, Graph targetGraph, ThriftClient client, long ns, boolean applyStringDistances, double similarityThreshold) {
        Graph matchedSourceGraph;
        Graph matchedTargetGraph;
        if (applyStringDistances) {

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
                vertexPermutations = Itertools.combinations(Itertools.iter(labelSet.iterator()), 2);
                distanceMap.putAll(getDistance(client, ns, vertexPermutations));
            }
            Generator<List<String>> edgePermutations;
            if (edgeSet.size() >= 2) {
                edgePermutations = Itertools.combinations(Itertools.iter(edgeSet.iterator()), 2);
                distanceMap.putAll(getDistance(client, ns, edgePermutations));
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

    private static Map<String, Map<String, Double>> getDistance(ThriftClient client, long ns, Generator<List<String>> vertexPermutations) {
        Map<String, Map<String, Double>> distanceMap = new HashMap<>();
        boolean end = false;

        String query = "SELECT * FROM alignments WHERE distance = 'geometricMean'";

        HqlResult hqlResult = null;
        try {
            hqlResult = client.hql_query(ns, query);
        } catch (TException e) {
            e.printStackTrace();
        }

        while(!end) {
            try {
                List<String> pair = vertexPermutations.next();

                // OntologyEquality

                String sourceNamespace = null;
                String targetNamespace = null;
                try {
                    URL sourceURL = new URL(pair.get(0).replace("\"", ""));
                    sourceNamespace = String.format("%s://%s%s", sourceURL.getProtocol(), sourceURL.getHost(), sourceURL.getPath());
                    URL targetURL = new URL(pair.get(1).replace("\"", ""));
                    targetNamespace = String.format("%s://%s%s", targetURL.getProtocol(), targetURL.getHost(), targetURL.getPath());
                } catch (MalformedURLException e) {
                    e.printStackTrace();
                }

                if ((sourceNamespace != null) && (targetNamespace != null)) {

                    if (!sourceNamespace.equals(targetNamespace)) {

                        for (Cell cell : hqlResult.getCells()) {
                            ByteBuffer sourceBuffer = client.get_cell(ns, "alignments", cell.getKey().getRow(), "source");
                            String source = new String(sourceBuffer.array(), sourceBuffer.position(), sourceBuffer.remaining());

                            if (source.equals(pair.get(0))) {
                                ByteBuffer targetBuffer = client.get_cell(ns, "alignments", cell.getKey().getRow(), "target");
                                String target = new String(targetBuffer.array(), targetBuffer.position(), targetBuffer.remaining());

                                if (target.equals(pair.get(1))) {
                                    ByteBuffer valueBuffer = client.get_cell(ns, "alignments", cell.getKey().getRow(), "value");
                                    double value = Double.valueOf(new String(valueBuffer.array(), valueBuffer.position(), valueBuffer.remaining()));
                                    if (!distanceMap.containsKey(pair.get(0))) {
                                        distanceMap.put(pair.get(0), new HashMap<String, Double>());
                                    }
                                    Map<String, Double> map = distanceMap.get(pair.get(0));
                                    map.put(pair.get(1), value);
                                    distanceMap.put(pair.get(0), map);
                                }
                            }
                        }
                    } else {
                        if (!distanceMap.containsKey(pair.get(0))) {
                            distanceMap.put(pair.get(0), new HashMap<String, Double>());
                        }
                        Map<String, Double> map = distanceMap.get(pair.get(0));
                        map.put(pair.get(1), 0.0);
                        distanceMap.put(pair.get(0), map);
                    }
                }

            } catch (NoSuchElementException e) {
                end = true;
            } catch (ClientException e) {
                e.printStackTrace();
            } catch (TException e) {
                e.printStackTrace();
            }
        }
        return distanceMap;
    }

    private static Graph getGraph(String graphName, ThriftClient client, long ns) {

        Graph graph = new Graph(graphName);

        String query = String.format("SELECT * from subgraphs where graph = '%s'", graphName);

        HqlResult hqlResult = null;
        try {
            hqlResult = client.hql_query(ns, query);
            if (hqlResult.getCells().size() > 0) {
                // Get vertices
                for (Cell cell : hqlResult.getCells()) {
                    ByteBuffer typeBuffer = client.get_cell(ns, "subgraphs", cell.getKey().getRow(), "type");
                    String type = new String(typeBuffer.array(), typeBuffer.position(), typeBuffer.remaining());
                    if (type.equals("vertex")) {
                        ByteBuffer idBuffer = client.get_cell(ns, "subgraphs", cell.getKey().getRow(), "id");
                        long id = Long.parseLong(new String(idBuffer.array(), idBuffer.position(), idBuffer.remaining()));

                        ByteBuffer labelBuffer = client.get_cell(ns, "subgraphs", cell.getKey().getRow(), "label");
                        String label = new String(labelBuffer.array(), labelBuffer.position(), labelBuffer.remaining());

                        Vertex vertex = new Vertex(label, id);
                        graph.addVertex(vertex);
                    }
                }
                // Get edges
                for (Cell cell : hqlResult.getCells()) {
                    ByteBuffer typeBuffer = client.get_cell(ns, "subgraphs", cell.getKey().getRow(), "type");
                    String type = new String(typeBuffer.array(), typeBuffer.position(), typeBuffer.remaining());
                    if (type.equals("edge")) {

                        ByteBuffer sourceBuffer = client.get_cell(ns, "subgraphs", cell.getKey().getRow(), "source");
                        long source = Long.parseLong(new String(sourceBuffer.array(), sourceBuffer.position(), sourceBuffer.remaining()));

                        ByteBuffer targetBuffer = client.get_cell(ns, "subgraphs", cell.getKey().getRow(), "target");
                        long target = Long.parseLong(new String(targetBuffer.array(), targetBuffer.position(), targetBuffer.remaining()));

                        ByteBuffer labelBuffer = client.get_cell(ns, "subgraphs", cell.getKey().getRow(), "label");
                        String label = new String(labelBuffer.array(), labelBuffer.position(), labelBuffer.remaining());

                        Vertex sourceVertex = graph.getVertex(source);
                        Vertex targetVertex = graph.getVertex(target);
                        Edge edge = new Edge(label, targetVertex);
                        sourceVertex.addEdge(edge);
                        graph.updateVertex(sourceVertex);
                    }
                }

            }
        } catch (TException e) {
            e.printStackTrace();
        }

        return graph;
    }
}
