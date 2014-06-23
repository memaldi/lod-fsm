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
import org.deustotech.internet.phd.framework.model.Edge;
import org.deustotech.internet.phd.framework.model.Graph;
import org.deustotech.internet.phd.framework.model.Vertex;

import java.io.IOException;
import java.util.*;

/**
 * Created by mikel (m.emaldi at deusto dot es) on 20/06/14.
 */
public class MatchSubgraphs {
    public static void run(double similarityThreshold) {
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

        Generator<List<String>> graphPermutations = Itertools.permutations(Itertools.iter(graphSet.iterator()), 2);
        boolean end = false;
        while(!end) {
            try {
                List<String> pair = graphPermutations.next();
                Graph sourceGraph = getGraph(pair.get(0), htable);
                Graph targetGraph = getGraph(pair.get(1), htable);
                List<Graph> matchedGraphs = matchGraphs(sourceGraph, targetGraph, conf, similarityThreshold);
            } catch (NoSuchElementException e) {
                end = true;
            }
        }
        try {
            htable.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static List<Graph> matchGraphs(Graph sourceGraph, Graph targetGraph, Configuration conf, double similarityThreshold) {
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

        Generator<List<String>> vertexPermutations = Itertools.permutations(Itertools.iter(labelSet.iterator()), 2);
        Generator<List<String>> edgePermutations = Itertools.permutations(Itertools.iter(edgeSet.iterator()), 2);

        Map<String, Map<String, Double>> scoreMap = new HashMap<>();
        scoreMap.putAll(getScores(table, vertexPermutations));
        scoreMap.putAll(getScores(table, edgePermutations));

        System.out.println(scoreMap);

        Map<String, String> replaceMap = new HashMap<>();

        for (String label : scoreMap.keySet()) {
            Map<String, Double> map = scoreMap.get(label);
            double maxScore = 0;
            String maxLabel = "";
            for (String key : map.keySet()) {
                if (map.get(key) > maxScore) {
                    maxScore = map.get(key);
                    maxLabel = key;
                }
            }
            if (1 - maxScore > similarityThreshold) {
                String uuid = UUID.randomUUID().toString();
                replaceMap.put(label, uuid);
                replaceMap.put(maxLabel, uuid);
            }
        }
        Graph matchedSourceGraph = getMatchedGraph(sourceGraph, replaceMap);
        Graph matchedTargetGraph = getMatchedGraph(sourceGraph, replaceMap);

        return null;
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

    private static Map<String, Map<String, Double>> getScores(HTable table, Generator<List<String>> vertexPermutations) {
        Map<String, Map<String, Double>> scoreMap = new HashMap<>();
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
                        if (!scoreMap.containsKey(pair.get(0))) {
                            scoreMap.put(pair.get(0), new HashMap<String, Double>());
                        }
                        Map<String, Double> map = scoreMap.get(pair.get(0));
                        map.put(pair.get(1), value);
                        scoreMap.put(pair.get(0), map);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } catch (NoSuchElementException e) {
                end = true;
            }
        }
        return scoreMap;
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
