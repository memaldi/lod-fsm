package org.deustotech.internet.phd.validation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

/**
 * Created by mikel (m.emaldi at deusto dot es) on 18/06/14.
 * This class tests the agreement between different distances.
 */
public class TestDistances {
    public static void run() {
        Logger logger = Logger.getLogger(TestDistances.class.getName());
        Configuration conf = HBaseConfiguration.create();
        HTable hTable = null;
        try {
            hTable = new HTable(conf, "alignments");
        } catch (IOException e) {
            e.printStackTrace();
        }

        Map<Double, Map<String, Map<String, Boolean>>> scoreMap = new HashMap<>();

        List<Filter> filterList = new ArrayList<>();
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("distance"), CompareFilter.CompareOp.NOT_EQUAL, Bytes.toBytes("geometricMean"));
        filterList.add(filter);
        FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL, filterList);
        Scan scan = new Scan();
        scan.setFilter(fl);
        try {
            ResultScanner scanner = hTable.getScanner(scan);
            Result result;
            while((result = scanner.next()) != null) {
                String distance = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("distance")));
                double value = Bytes.toDouble(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("value")));
                String source = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("source")));
                String target = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("target")));

                for (double i=0; i <= 1; i += 0.1) {
                    if (!scoreMap.containsKey(i)) {
                        scoreMap.put(i, new HashMap<String, Map<String, Boolean>>());
                    }
                    if (!scoreMap.get(i).containsKey(String.format("%s-%s", source, target))) {
                        Map<String, Boolean> map = new HashMap<>();
                        scoreMap.get(i).put(String.format("%s-%s", source, target), map);
                    }
                    Map<String, Boolean> map = scoreMap.get(i).get(String.format("%s-%s", source, target));
                    if (value > i) {
                        map.put(distance, true);
                    } else {
                        map.put(distance, false);
                    }
                    scoreMap.get(i).put(String.format("%s-%s", source, target), map);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        int raters = 5;
        Map<Double, Double> kMap = new HashMap<>();
        for (double threshold : scoreMap.keySet()) {
            float P = 0;
            int trueRate = 1;
            int falseRate = 1;
            for(String pair : scoreMap.get(threshold).keySet()) {
                Map<String, Boolean> map = scoreMap.get(threshold).get(pair);
                int accum = 0;
                for (String distance : map.keySet()) {
                    if (map.get(distance)) {
                        accum += 1;
                        trueRate += 1;
                    } else {
                        falseRate += 1;
                    }
                }
                double localP = 1 / raters * (raters - 1) * (Math.pow(accum, 2) - raters);
                P += localP;

            }
            float Ptrue = trueRate / (trueRate + falseRate);
            float Pfalse = falseRate / (trueRate + falseRate);
            double Pe = Math.pow(Ptrue, 2) + Math.pow(Pfalse, 2);

            P = 1 / scoreMap.get(threshold).keySet().size() * P;
            double k = (P - Pe) / (1 - Pe);
            kMap.put(threshold, k);
        }

        for (double threshold : kMap.keySet()) {
            System.out.println(String.format("Agreement for %s threshold: %s", threshold, kMap.get(threshold)));
        }

    }
}
