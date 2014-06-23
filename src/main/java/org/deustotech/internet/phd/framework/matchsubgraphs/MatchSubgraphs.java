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
    public static void run() {
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
        Graph graph = new Graph();
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
