package org.deustotech.internet.phd.framework.loadsubgraphs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.util.UUID;
import java.util.logging.Logger;

/**
 * Created by mikel (m.emaldi at deusto dot es) on 17/06/14.
 */
public class LoadSubgraphs {
    public static void run(String inputDir) {
        Logger logger = Logger.getLogger(LoadSubgraphs.class.getName());
        File folder = new File(inputDir);
        Configuration conf = HBaseConfiguration.create();
        HTable table = null;
        try {
            logger.info("Connecting to table...");
            table = new HTable(conf, "subgraphs");
        } catch (IOException e) {
            logger.info("Table not found! Creating new table...");
            HBaseAdmin hbase = null;
            try {
                hbase = new HBaseAdmin(conf);
                HTableDescriptor desc = new HTableDescriptor("subgraphs");
                HColumnDescriptor meta = new HColumnDescriptor("cf".getBytes());
                desc.addFamily(meta);
                hbase.createTable(desc);
                table = new HTable(conf, "subgraphs");
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        }

        for(File file : folder.listFiles()) {
            try {
                FileReader fileReader = new FileReader(file);
                BufferedReader br = new BufferedReader(fileReader);
                String line;
                while((line = br.readLine()) != null) {
                    if (line.startsWith("v")) {
                        String copyLine = line.replace("\n", "");
                        String[] sline = copyLine.split(" ");
                        Put put = new Put(Bytes.toBytes(UUID.randomUUID().toString()));
                        put.add(Bytes.toBytes("cf"), Bytes.toBytes("type"), Bytes.toBytes("vertex"));
                        put.add(Bytes.toBytes("cf"), Bytes.toBytes("id"), Bytes.toBytes(Long.parseLong(sline[1])));
                        put.add(Bytes.toBytes("cf"), Bytes.toBytes("label"), Bytes.toBytes(sline[2]));
                        put.add(Bytes.toBytes("cf"), Bytes.toBytes("graph"), Bytes.toBytes(file.getName()));
                        table.put(put);
                    } else if (line.startsWith("d")) {
                        String copyLine = line.replace("\n", "");
                        String[] sline = copyLine.split(" ");
                        Put put = new Put(Bytes.toBytes(UUID.randomUUID().toString()));
                        put.add(Bytes.toBytes("cf"), Bytes.toBytes("type"), Bytes.toBytes("edge"));
                        put.add(Bytes.toBytes("cf"), Bytes.toBytes("source"), Bytes.toBytes(Long.parseLong(sline[1])));
                        put.add(Bytes.toBytes("cf"), Bytes.toBytes("target"), Bytes.toBytes(Long.parseLong(sline[2])));
                        put.add(Bytes.toBytes("cf"), Bytes.toBytes("label"), Bytes.toBytes(sline[3]));
                        put.add(Bytes.toBytes("cf"), Bytes.toBytes("graph"), Bytes.toBytes(file.getName()));
                        table.put(put);
                    }
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
