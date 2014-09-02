package org.deustotech.internet.phd.framework.generatealignments;

import net.ericaro.neoitertools.Generator;
import net.ericaro.neoitertools.Itertools;
import org.apache.hadoop.conf.Configuration;


import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.logging.Logger;
import fr.inrialpes.exmo.ontosim.string.StringDistances;
import fr.inrialpes.exmo.ontosim.string.JWNLDistances;
import org.apache.thrift.TException;
import org.hypertable.thrift.ThriftClient;
import org.hypertable.thriftgen.*;


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
        ThriftClient client = null;
        try {
            client = ThriftClient.create("localhost", 15867);
        } catch (TException e) {
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

        Schema schema = new Schema();

        Map columnFamilies = new HashMap();

        ColumnFamilySpec cf = new ColumnFamilySpec();
        cf.setName("source");
        columnFamilies.put("source", cf);

        cf = new ColumnFamilySpec();
        cf.setName("target");
        columnFamilies.put("target", cf);

        cf = new ColumnFamilySpec();
        cf.setName("distance");
        columnFamilies.put("distance", cf);

        cf = new ColumnFamilySpec();
        cf.setName("value");
        columnFamilies.put("value", cf);

        schema.setColumn_families(columnFamilies);

        try {
            client.table_create(ns, "alignments", schema);
        } catch (TException e) {
            e.printStackTrace();
        }

        logger.info("Aligning vertices...");
        generateSimilarities(client, ns, "vertex", wordnetDir, wordnetVersion);
        logger.info("Aligning edges...");
        generateSimilarities(client, ns, "edge", wordnetDir, wordnetVersion);
        logger.info("Done!");

        client.close();

    }

    private static void generateSimilarities(ThriftClient client, long ns, String type, String wordnetDir, String wordnetVersion) {

        String query = String.format("SELECT * from subgraphs where type = '%s'", type);

        Set<String> vertexSet = new HashSet<>();
        try {
            HqlResult hqlResult = client.hql_query(ns, query);
            if (hqlResult.getCells().size() > 0) {
                for (Cell cell : hqlResult.getCells()) {
                    ByteBuffer labelBuffer = client.get_cell(ns, "subgraphs", cell.getKey().getRow(), "label");
                    String label = new String(labelBuffer.array(), labelBuffer.position(), labelBuffer.remaining());
                    vertexSet.add(label.replace("<", "").replace(">", ""));
                }
            }
        } catch (ClientException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }

        Generator<List<String>> labelPermutations = Itertools.permutations(Itertools.iter(vertexSet.iterator()), 2);
        List<String> pair;
        Class<StringDistances> stringDistancesClass = StringDistances.class;
        JWNLDistances jwnlDistances = new JWNLDistances();
        jwnlDistances.Initialize(wordnetDir, wordnetVersion);
        Class[] cArg = new Class[2];
        cArg[0] = String.class;
        cArg[1] = String.class;
        List<Cell> cells = new ArrayList<>();
        try {
            while ((pair = labelPermutations.next()) != null) {
                if (!getNamespace(pair.get(0)).equals(getNamespace(pair.get(1)))) {
                    double accum = 0.0;

                    for (String strDistance : STRING_DISTANCES) {
                        Method method = stringDistancesClass.getMethod(strDistance, cArg);
                        double distance = (double) method.invoke(stringDistancesClass, getLocalName(pair.get(0)), getLocalName(pair.get(1)));

                        String keyID = UUID.randomUUID().toString();
                        Key key = new Key();
                        key.setRow(keyID);
                        key.setColumn_family("source");
                        Cell cell = new Cell();
                        cell.setKey(key);

                        try {
                            cell.setValue(pair.get(0).getBytes("UTF-8"));
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
                            cell.setValue(pair.get(1).getBytes("UTF-8"));
                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                        }

                        cells.add(cell);

                        key = new Key();
                        key.setRow(keyID);
                        key.setColumn_family("distance");
                        cell = new Cell();
                        cell.setKey(key);

                        try {
                            cell.setValue(strDistance.getBytes("UTF-8"));
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
                            cell.setValue(String.valueOf(distance).getBytes("UTF-8"));
                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                        }

                        cells.add(cell);

                        accum += distance;
                    }
                    for (String jwnlDistance : JWNL_DISTANCES) {
                        Method method = jwnlDistances.getClass().getMethod(jwnlDistance, cArg);
                        double distance = (double) method.invoke(jwnlDistances, getLocalName(pair.get(0)), getLocalName(pair.get(1)));

                        String keyID = UUID.randomUUID().toString();
                        Key key = new Key();
                        key.setRow(keyID);
                        key.setColumn_family("source");
                        Cell cell = new Cell();
                        cell.setKey(key);

                        try {
                            cell.setValue(pair.get(0).getBytes("UTF-8"));
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
                            cell.setValue(pair.get(1).getBytes("UTF-8"));
                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                        }

                        cells.add(cell);

                        key = new Key();
                        key.setRow(keyID);
                        key.setColumn_family("distance");
                        cell = new Cell();
                        cell.setKey(key);

                        try {
                            cell.setValue(jwnlDistance.getBytes("UTF-8"));
                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                        }

                        cells.add(cell);

                        cells.add(cell);

                        key = new Key();
                        key.setRow(keyID);
                        key.setColumn_family("value");
                        cell = new Cell();
                        cell.setKey(key);

                        try {
                            cell.setValue(String.valueOf(distance).getBytes("UTF-8"));
                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                        }

                        cells.add(cell);

                        accum += distance;
                    }
                    double geometricMean = accum / (STRING_DISTANCES.length + JWNL_DISTANCES.length);

                    String keyID = UUID.randomUUID().toString();
                    Key key = new Key();
                    key.setRow(keyID);
                    key.setColumn_family("source");
                    Cell cell = new Cell();
                    cell.setKey(key);

                    try {
                        cell.setValue(pair.get(0).getBytes("UTF-8"));
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
                        cell.setValue(pair.get(1).getBytes("UTF-8"));
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }

                    cells.add(cell);

                    key = new Key();
                    key.setRow(keyID);
                    key.setColumn_family("distance");
                    cell = new Cell();
                    cell.setKey(key);

                    try {
                        cell.setValue("geometricMean".getBytes("UTF-8"));
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
                        cell.setValue(String.valueOf(geometricMean).getBytes("UTF-8"));
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }

                    cells.add(cell);
                }
            }
        } catch (NoSuchElementException e) {
            // Well, permutations.next() do not return null when the last element is reached.
            try {
                client.set_cells(ns, "alignments", cells);
            } catch (TException e1) {
                e1.printStackTrace();
            }
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
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
                return sURI[sURI.length - 1];
            } else {
                return URI;
            }
        }
    }
}
