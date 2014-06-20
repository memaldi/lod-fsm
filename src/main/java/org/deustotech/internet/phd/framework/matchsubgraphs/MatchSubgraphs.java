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
import org.apache.hadoop.yarn.state.Graph;

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
        } catch (IOException e) {
            e.printStackTrace();
        }
        Generator<List<String>> graphPermutations = Itertools.permutations(Itertools.iter(graphSet.iterator()), 2);
        boolean end = false;
        while(!end) {
            try {
                List<String> pair = graphPermutations.next();
                
            } catch (NoSuchElementException e) {
                end = true;
            }
        }
    }
}
