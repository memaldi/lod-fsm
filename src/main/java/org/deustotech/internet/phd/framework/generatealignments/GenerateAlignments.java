package org.deustotech.internet.phd.framework.generatealignments;

import net.ericaro.neoitertools.Generator;
import net.ericaro.neoitertools.Itertools;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.logging.Logger;
import fr.inrialpes.exmo.ontosim.string.StringDistances;
import fr.inrialpes.exmo.ontosim.string.JWNLDistances;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by mikel (m.emaldi at deusto dot es) on 18/06/14.
 */
public class GenerateAlignments {

    static String[] STRING_DISTANCES =  {
            "subStringDistance",
            "equalDistance",
            "levenshteinDistance",
            "smoaDistance",
    };

    static String[] JWNL_DISTANCES = {
            "basicSynonymDistance"
    };

    public static void run(String wordnetDir, String wordnetVersion) {
        Logger logger = Logger.getLogger(GenerateAlignments.class.getName());
        Configuration conf = HBaseConfiguration.create();
        HTable alignmentTable = null;
        HTable subgraphTable = null;
        logger.info("Creating table \"alignments\"...");
        HBaseAdmin hbase = null;
        try {
            hbase = new HBaseAdmin(conf);
            HTableDescriptor desc = new HTableDescriptor("alignments");
            HColumnDescriptor meta = new HColumnDescriptor("cf".getBytes());
            desc.addFamily(meta);
            hbase.createTable(desc);
        } catch (IOException e) {
            logger.info("Table \"alignments\" already exists!");
        }
        try {
            subgraphTable = new HTable(conf, "subgraphs");
            alignmentTable = new HTable(conf, "alignments");
        } catch (IOException e) {
            e.printStackTrace();
        }

        logger.info("Aligning vertices...");
        generateSimilarities(alignmentTable, subgraphTable, "vertex", wordnetDir, wordnetVersion);
        logger.info("Aligning edges...");
        generateSimilarities(alignmentTable, subgraphTable, "edge", wordnetDir, wordnetVersion);
        logger.info("Done!");
        try {
            alignmentTable.close();
            subgraphTable.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static void generateSimilarities(HTable alignmentTable, HTable subgraphTable, String type, String wordnetDir, String wordnetVersion) {
        List<Filter> filterList = new ArrayList<>();
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("type"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(type));
        filter.setFilterIfMissing(true);
        filterList.add(filter);
        FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL, filterList);
        Scan scan = new Scan();
        scan.setFilter(fl);
        Set<String> vertexSet = new HashSet<>();
        try {
            ResultScanner scanner = subgraphTable.getScanner(scan);
            Result result;
            while ((result = scanner.next()) != null) {
                String label = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("label")));
                vertexSet.add(label.replace("<", "").replace(">", ""));
            }
            scanner.close();

            Generator<List<String>> labelPermutations = Itertools.permutations(Itertools.iter(vertexSet.iterator()), 2);
            List<String> pair;
            Class<StringDistances> stringDistancesClass = StringDistances.class;
            JWNLDistances jwnlDistances = new JWNLDistances();
            jwnlDistances.Initialize(wordnetDir, wordnetVersion);
            Class[] cArg = new Class[2];
            cArg[0] = String.class;
            cArg[1] = String.class;
            try {
                while ((pair = labelPermutations.next()) != null) {
                    if (!getNamespace(pair.get(0)).equals(getNamespace(pair.get(1)))) {
                        double accum = 0.0;

                        for (String strDistance : STRING_DISTANCES) {
                            Method method = stringDistancesClass.getMethod(strDistance, cArg);
                            double distance = (double) method.invoke(stringDistancesClass, getLocalName(pair.get(0)), getLocalName(pair.get(1)));
                            Put put = new Put(Bytes.toBytes(UUID.randomUUID().toString()));
                            put.add(Bytes.toBytes("cf"), Bytes.toBytes("source"), Bytes.toBytes(pair.get(0)));
                            put.add(Bytes.toBytes("cf"), Bytes.toBytes("target"), Bytes.toBytes(pair.get(1)));
                            put.add(Bytes.toBytes("cf"), Bytes.toBytes("distance"), Bytes.toBytes(strDistance));
                            put.add(Bytes.toBytes("cf"), Bytes.toBytes("value"), Bytes.toBytes(distance));
                            alignmentTable.put(put);
                            accum += distance;
                        }
                        for (String jwnlDistance : JWNL_DISTANCES) {
                            Method method = jwnlDistances.getClass().getMethod(jwnlDistance, cArg);
                            double distance = (double) method.invoke(jwnlDistances, getLocalName(pair.get(0)), getLocalName(pair.get(1)));
                            Put put = new Put(Bytes.toBytes(UUID.randomUUID().toString()));
                            put.add(Bytes.toBytes("cf"), Bytes.toBytes("source"), Bytes.toBytes(pair.get(0)));
                            put.add(Bytes.toBytes("cf"), Bytes.toBytes("target"), Bytes.toBytes(pair.get(1)));
                            put.add(Bytes.toBytes("cf"), Bytes.toBytes("distance"), Bytes.toBytes(jwnlDistance));
                            put.add(Bytes.toBytes("cf"), Bytes.toBytes("value"), Bytes.toBytes(distance));
                            alignmentTable.put(put);
                            accum += distance;
                        }
                        Put put = new Put(Bytes.toBytes(UUID.randomUUID().toString()));
                        double geometricMean = accum / (STRING_DISTANCES.length + JWNL_DISTANCES.length);
                        put.add(Bytes.toBytes("cf"), Bytes.toBytes("source"), Bytes.toBytes(pair.get(0)));
                        put.add(Bytes.toBytes("cf"), Bytes.toBytes("target"), Bytes.toBytes(pair.get(1)));
                        put.add(Bytes.toBytes("cf"), Bytes.toBytes("distance"), Bytes.toBytes("geometricMean"));
                        put.add(Bytes.toBytes("cf"), Bytes.toBytes("value"), Bytes.toBytes(geometricMean));
                        alignmentTable.put(put);
                    }
                }
            } catch (NoSuchElementException e) {
                // Well, permutations.next() do not return null when the last element is reached.
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String getNamespace(String URI) {
        if (URI.contains("#")) {
            return URI.split("#")[0];
        } else {
            String[] sURI = URI.split("/");
            String result = "";
            for (int i = 0; i < sURI.length - 1; i++) {
                result += sURI[i] + "/";
            }
            return result;
        }
    }

    private static String getLocalName(String URI) {
        if (URI.contains("#")) {
            return URI.split("#")[1];
        } else {
            String[] sURI = URI.split("/");
            if (sURI.length >= 3) {
                return sURI[sURI.length - 2];
            } else {
                return URI;
            }
        }
    }
}
