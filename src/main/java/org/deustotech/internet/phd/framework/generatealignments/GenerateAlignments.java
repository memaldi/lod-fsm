package org.deustotech.internet.phd.framework.generatealignments;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

/**
 * Created by mikel (m.emaldi at deusto dot es) on 18/06/14.
 */
public class GenerateAlignments {
    public static void run() {
        Logger logger = Logger.getLogger(GenerateAlignments.class.getName());
        Configuration conf = HBaseConfiguration.create();
        HTable table = null;
        try {
            logger.info("Connecting to table...");
            table = new HTable(conf, "alignments");
        } catch (IOException e) {
            logger.info("Table not found! Creating new table...");
            HBaseAdmin hbase = null;
            try {
                hbase = new HBaseAdmin(conf);
                HTableDescriptor desc = new HTableDescriptor("alignments");
                HColumnDescriptor meta = new HColumnDescriptor("cf".getBytes());
                desc.addFamily(meta);
                hbase.createTable(desc);
                table = new HTable(conf, "alignments");
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        }
        
    }
}
