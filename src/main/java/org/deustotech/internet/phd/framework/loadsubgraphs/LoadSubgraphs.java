package org.deustotech.internet.phd.framework.loadsubgraphs;



import org.apache.hadoop.hbase.util.Bytes;
import org.apache.thrift.TException;
import org.hypertable.thrift.ThriftClient;
import org.hypertable.thriftgen.Cell;
import org.hypertable.thriftgen.ColumnFamilySpec;
import org.hypertable.thriftgen.Key;
import org.hypertable.thriftgen.Schema;

import java.io.*;
import java.util.*;
import java.util.logging.Logger;

/**
 * Created by mikel (m.emaldi at deusto dot es) on 17/06/14.
 */
public class LoadSubgraphs {
    public static void run(String inputDir) {
        Logger logger = Logger.getLogger(LoadSubgraphs.class.getName());
        File folder = new File(inputDir);
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
        cf.setName("type");
        cf.setValue_index(true);
        columnFamilies.put("type", cf);

        cf = new ColumnFamilySpec();
        cf.setName("id");
        columnFamilies.put("id", cf);

        cf = new ColumnFamilySpec();
        cf.setName("label");
        columnFamilies.put("label", cf);

        cf = new ColumnFamilySpec();
        cf.setName("graph");
        cf.setValue_index(true);
        columnFamilies.put("graph", cf);

        cf = new ColumnFamilySpec();
        cf.setName("source");
        columnFamilies.put("source", cf);

        cf = new ColumnFamilySpec();
        cf.setName("target");
        columnFamilies.put("target", cf);

        schema.setColumn_families(columnFamilies);

        try {
            client.table_create(ns, "subgraphs", schema);
        } catch (TException e) {
            e.printStackTrace();
        }
        List<Cell> cells = new ArrayList<>();
        for(File file : folder.listFiles()) {
            try {
                FileReader fileReader = new FileReader(file);
                BufferedReader br = new BufferedReader(fileReader);
                String line;
                while((line = br.readLine()) != null) {
                    if (line.startsWith("v")) {
                        String copyLine = line.replace("\n", "");
                        String[] sline = copyLine.split(" ");

                        String keyID = UUID.randomUUID().toString();
                        Key key = new Key();
                        key.setRow(keyID);
                        key.setColumn_family("type");
                        Cell cell = new Cell();
                        cell.setKey(key);

                        try {
                            cell.setValue("vertex".getBytes("UTF-8"));
                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                        }

                        cells.add(cell);

                        key = new Key();
                        key.setRow(keyID);
                        key.setColumn_family("id");
                        cell = new Cell();
                        cell.setKey(key);

                        try {
                            cell.setValue(sline[1].getBytes("UTF-8"));
                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                        }

                        cells.add(cell);

                        key = new Key();
                        key.setRow(keyID);
                        key.setColumn_family("label");
                        cell = new Cell();
                        cell.setKey(key);

                        try {
                            cell.setValue(sline[2].replace("<", "").replace(">", "").getBytes("UTF-8"));
                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                        }

                        cells.add(cell);

                        key = new Key();
                        key.setRow(keyID);
                        key.setColumn_family("graph");
                        cell = new Cell();
                        cell.setKey(key);

                        try {
                            cell.setValue(file.getName().getBytes("UTF-8"));
                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                        }

                        cells.add(cell);


                    } else if (line.startsWith("d")) {
                        String copyLine = line.replace("\n", "");
                        String[] sline = copyLine.split(" ");

                        String keyID = UUID.randomUUID().toString();
                        Key key = new Key();
                        key.setRow(keyID);
                        key.setColumn_family("type");
                        Cell cell = new Cell();
                        cell.setKey(key);

                        try {
                            cell.setValue("edge".getBytes("UTF-8"));
                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                        }

                        cells.add(cell);

                        key = new Key();
                        key.setRow(keyID);
                        key.setColumn_family("source");
                        cell = new Cell();
                        cell.setKey(key);

                        try {
                            cell.setValue(sline[1].getBytes("UTF-8"));
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
                            cell.setValue(sline[2].getBytes("UTF-8"));
                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                        }

                        cells.add(cell);

                        key = new Key();
                        key.setRow(keyID);
                        key.setColumn_family("label");
                        cell = new Cell();
                        cell.setKey(key);

                        try {
                            cell.setValue(sline[3].replace("<", "").replace(">", "").getBytes("UTF-8"));
                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                        }

                        cells.add(cell);

                        key = new Key();
                        key.setRow(keyID);
                        key.setColumn_family("graph");
                        cell = new Cell();
                        cell.setKey(key);

                        try {
                            cell.setValue(file.getName().getBytes("UTF-8"));
                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                        }

                        cells.add(cell);

                    }
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            client.set_cells(ns, "subgraphs", cells);
        } catch (TException e) {
            e.printStackTrace();
        }
        client.close();
        logger.info("Done!");
    }
}
