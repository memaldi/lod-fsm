package org.deustotech.internet.phd.framework.rdf2subdue;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.vocabulary.RDF;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.client.utils.URIBuilder;
import redis.clients.jedis.Jedis;
import virtuoso.jena.driver.VirtGraph;
import virtuoso.jena.driver.VirtuosoQueryExecution;
import virtuoso.jena.driver.VirtuosoQueryExecutionFactory;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.logging.Logger;

/**
 * Created by memaldi (m.emaldi at deusto dot es) on 14/06/14.
 */
public class RDF2Subdue {

    public static void run(String dataset, String outputDir) {
        generateId(dataset);
        writeFile(dataset, outputDir);
    }

    private static void writeFile(String dataset, String outputDir) {
        Logger logger = Logger.getLogger(RDF2Subdue.class.getName());
        Configuration conf = HBaseConfiguration.create();
        HTable table = null;
        try {
            table = new HTable(conf, dataset);
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("Writing files...");
        String dir = String.format("%s/%s", outputDir, dataset);
        new File(dir).mkdir();
        logger.info("Counting vertices...");
        List<Filter> filterList = new ArrayList<>();
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("type"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes("vertex"));
        filter.setFilterIfMissing(true);
        filterList.add(filter);
        FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL, filterList);
        Scan scan = new Scan();
        scan.setFilter(fl);

        try {
            ResultScanner scanner = table.getScanner(scan);
            Result scannerResult;
            long total = 0;
            while((scannerResult = scanner.next()) != null) {
                total++;
            }
            scanner.close();

            boolean end = false;
            int limit = 1000;
            long lowerLimit = 0;
            long upperLimit = 1000;
            int count = 1;

            while (!end) {
                logger.info(String.format("Writing %s_%s.g...", dataset, count));
                filterList = new ArrayList<>();
                SingleColumnValueFilter lowerFilter = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("id"), CompareFilter.CompareOp.GREATER, Bytes.toBytes(lowerLimit));
                lowerFilter.setFilterIfMissing(true);
                SingleColumnValueFilter upperFilter = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("id"), CompareFilter.CompareOp.LESS_OR_EQUAL, Bytes.toBytes(upperLimit));
                upperFilter.setFilterIfMissing(true);
                filterList.add(lowerFilter);
                filterList.add(upperFilter);
                fl = new FilterList(FilterList.Operator.MUST_PASS_ALL, filterList);
                scan = new Scan();
                scan.setFilter(fl);
                scanner = table.getScanner(scan);
                File file = new File(String.format("%s/%s_%s.g", dir, dataset, count));
                FileWriter fw = new FileWriter(file.getAbsoluteFile());
                BufferedWriter bw = new BufferedWriter(fw);
                while ((scannerResult = scanner.next()) != null) {
                    long id = Bytes.toLong(scannerResult.getValue(Bytes.toBytes("cf"), Bytes.toBytes("id")));
                    String label = Bytes.toString(scannerResult.getValue(Bytes.toBytes("cf"), Bytes.toBytes("label")));
                    bw.write(String.format("v %s %s\n", id, label));
                }
                scanner.close();
                bw.flush();

                filterList = new ArrayList<>();
                SingleColumnValueFilter sourceLowerFilter = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("source"), CompareFilter.CompareOp.GREATER, Bytes.toBytes(lowerLimit));
                sourceLowerFilter.setFilterIfMissing(true);
                SingleColumnValueFilter sourceUpperFilter = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("source"), CompareFilter.CompareOp.LESS_OR_EQUAL, Bytes.toBytes(upperLimit));
                sourceUpperFilter.setFilterIfMissing(true);
                SingleColumnValueFilter targetLowerFilter = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("target"), CompareFilter.CompareOp.GREATER, Bytes.toBytes(lowerLimit));
                targetLowerFilter.setFilterIfMissing(true);
                SingleColumnValueFilter targetUpperFilter = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("target"), CompareFilter.CompareOp.LESS_OR_EQUAL, Bytes.toBytes(upperLimit));
                targetUpperFilter.setFilterIfMissing(true);

                filterList.add(sourceLowerFilter);
                filterList.add(sourceUpperFilter);
                filterList.add(targetLowerFilter);
                filterList.add(targetUpperFilter);

                fl = new FilterList(FilterList.Operator.MUST_PASS_ALL, filterList);
                scan = new Scan();
                scan.setFilter(fl);
                scanner = table.getScanner(scan);

                while ((scannerResult = scanner.next()) != null) {
                    long source = Bytes.toLong(scannerResult.getValue(Bytes.toBytes("cf"), Bytes.toBytes("source")));
                    long target = Bytes.toLong(scannerResult.getValue(Bytes.toBytes("cf"), Bytes.toBytes("target")));
                    String label = Bytes.toString(scannerResult.getValue(Bytes.toBytes("cf"), Bytes.toBytes("label")));
                    bw.write(String.format("d %s %s %s\n", source, target, label));
                }
                bw.flush();
                bw.close();

                lowerLimit += limit;
                upperLimit += limit;
                count ++;
                if (lowerLimit > total) {
                    end = true;
                }
            }

            logger.info("End!");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void generateId(String dataset) {
        Logger logger = Logger.getLogger(RDF2Subdue.class.getName());
        logger.info("Initializing...");
        Properties prop = new Properties();
        InputStream input;
        try {
            input = RDF2Subdue.class.getResourceAsStream("/config.properties");
            prop.load(input);
            input.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        URI connectionURL = null;
        try {
            connectionURL = new URIBuilder().setScheme("jdbc:virtuoso").setHost(prop.getProperty("virtuoso_host"))
                    .setPort(Integer.parseInt(prop.getProperty("virtuoso_port"))).build();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }


        Configuration conf = HBaseConfiguration.create();
        HConnection connection = null;
        HTable table = null;
        try {
            logger.info("Connecting to HBase...");
            connection = HConnectionManager.createConnection(conf);
            logger.info(String.format("Creating table %s...", dataset));
            HBaseAdmin hbase = new HBaseAdmin(conf);
            HTableDescriptor desc = new HTableDescriptor(dataset);
            HColumnDescriptor meta = new HColumnDescriptor("cf".getBytes());
            desc.addFamily(meta);
            hbase.createTable(desc);
        } catch (IOException e) {
            logger.info(String.format("Table %s already exists!", dataset));
        }
        try {
            table = new HTable(conf, dataset);
        } catch (IOException e) {
            e.printStackTrace();
        }

        long offset = 0;
        Jedis jedis = new Jedis("localhost");
        if(jedis.exists(String.format("rdf2subdue:%s:offset", dataset))) {
            offset = Long.parseLong(jedis.get((String.format("rdf2subdue:%s:offset", dataset))));
        }

        VirtGraph graph = new VirtGraph("http://" + dataset, connectionURL.toString(), prop.getProperty("virtuoso_user"), prop.getProperty("virtuoso_password"));
        Query query = QueryFactory.create("SELECT DISTINCT ?s ?p ?o ?class WHERE { ?s ?p ?o OPTIONAL { ?o a ?class } } ORDER BY ?s OFFSET " + offset);
        VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(query, graph);
        ResultSet results = vqe.execSelect();
        logger.info("Generating vertices and edges ids...");

        long id = 1;

        while (results.hasNext()) {
            QuerySolution result = results.next();
            RDFNode subject = result.get("s");
            RDFNode predicate = result.get("p");
            RDFNode object = result.get("o");

            if (predicate.equals(RDF.type)) {
                Put put = new Put(Bytes.toBytes(UUID.randomUUID().toString()));
                put.add(Bytes.toBytes("cf"), Bytes.toBytes("uri"), Bytes.toBytes(subject.toString()));
                put.add(Bytes.toBytes("cf"), Bytes.toBytes("label"), Bytes.toBytes(object.toString()));
                put.add(Bytes.toBytes("cf"), Bytes.toBytes("type"), Bytes.toBytes("vertex"));
                put.add(Bytes.toBytes("cf"), Bytes.toBytes("id"), Bytes.toBytes(id));
                try {
                    table.put(put);
                } catch (InterruptedIOException e) {
                    e.printStackTrace();
                } catch (RetriesExhaustedWithDetailsException e) {
                    e.printStackTrace();
                }
                id++;
            } else if (object.isResource()) {
                List<Filter> filterList = new ArrayList<Filter>();
                SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("uri"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(object.toString()));
                filter.setFilterIfMissing(true);
                filterList.add(filter);
                FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL, filterList);
                Scan scan = new Scan();
                scan.setFilter(fl);
                try {
                    ResultScanner scanner = table.getScanner(scan);
                    Result scannerResult;
                    List<Long> idList = new ArrayList<Long>();

                    while((scannerResult = scanner.next()) != null) {
                        byte[] objectId = scannerResult.getValue(Bytes.toBytes("cf"), Bytes.toBytes("id"));
                        idList.add(Bytes.toLong(objectId));
                    }
                    byte[] sourceId = null;
                    List<Filter> filterList2 = new ArrayList<Filter>();
                    SingleColumnValueFilter filter2 = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("uri"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(subject.toString()));
                    filter2.setFilterIfMissing(true);
                    filterList2.add(filter2);
                    Filter fl2 = new FilterList(FilterList.Operator.MUST_PASS_ALL, filterList2);
                    Scan scan2 = new Scan();
                    scan2.setFilter(fl2);
                    ResultScanner scanner2 = table.getScanner(scan2);
                    Result scannerResult2;

                    while((scannerResult2 = scanner2.next()) != null) {
                        sourceId = scannerResult2.getValue(Bytes.toBytes("cf"), Bytes.toBytes("id"));
                    }
                    scanner2.close();
                    if (idList.size() == 0) {
                        if (result.contains("class")) {
                            RDFNode rdfType = result.get("class");
                            Put put = new Put(Bytes.toBytes(UUID.randomUUID().toString()));
                            put.add(Bytes.toBytes("cf"), Bytes.toBytes("uri"), Bytes.toBytes(object.toString()));
                            put.add(Bytes.toBytes("cf"), Bytes.toBytes("label"), Bytes.toBytes(rdfType.toString()));
                            put.add(Bytes.toBytes("cf"), Bytes.toBytes("type"), Bytes.toBytes("vertex"));
                            put.add(Bytes.toBytes("cf"), Bytes.toBytes("id"), Bytes.toBytes(id));
                        } else {
                            Put put = new Put(Bytes.toBytes(UUID.randomUUID().toString()));
                            put.add(Bytes.toBytes("cf"), Bytes.toBytes("label"), Bytes.toBytes(object.toString()));
                            put.add(Bytes.toBytes("cf"), Bytes.toBytes("type"), Bytes.toBytes("vertex"));
                            put.add(Bytes.toBytes("cf"), Bytes.toBytes("id"), Bytes.toBytes(id));
                            table.put(put);
                        }

                        Put put = new Put(Bytes.toBytes(UUID.randomUUID().toString()));
                        put.add(Bytes.toBytes("cf"), Bytes.toBytes("source"), sourceId);
                        put.add(Bytes.toBytes("cf"), Bytes.toBytes("target"), Bytes.toBytes(id));
                        put.add(Bytes.toBytes("cf"), Bytes.toBytes("label"), Bytes.toBytes(predicate.toString()));
                        put.add(Bytes.toBytes("cf"), Bytes.toBytes("type"), Bytes.toBytes("edge"));
                        table.put(put);
                        id++;
                    } else {
                        for (Long targetId : idList) {
                            Put put = new Put(Bytes.toBytes(UUID.randomUUID().toString()));
                            put.add(Bytes.toBytes("cf"), Bytes.toBytes("source"), sourceId);
                            put.add(Bytes.toBytes("cf"), Bytes.toBytes("target"), Bytes.toBytes(targetId));
                            put.add(Bytes.toBytes("cf"), Bytes.toBytes("label"), Bytes.toBytes(predicate.toString()));
                            put.add(Bytes.toBytes("cf"), Bytes.toBytes("type"), Bytes.toBytes("edge"));
                            table.put(put);
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }

            } else {
                Put put = new Put(Bytes.toBytes(UUID.randomUUID().toString()));
                put.add(Bytes.toBytes("cf"), Bytes.toBytes("label"), Bytes.toBytes(object.toString()));
                put.add(Bytes.toBytes("cf"), Bytes.toBytes("type"), Bytes.toBytes("vertex"));
                put.add(Bytes.toBytes("cf"), Bytes.toBytes("id"), Bytes.toBytes(id));
                try {
                    table.put(put);
                } catch (InterruptedIOException e) {
                    e.printStackTrace();
                } catch (RetriesExhaustedWithDetailsException e) {
                    e.printStackTrace();
                }

                List<Filter> filterList = new ArrayList<Filter>();
                SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("uri"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(subject.toString()));
                filter.setFilterIfMissing(true);
                filterList.add(filter);
                FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL, filterList);
                Scan scan = new Scan();
                scan.setFilter(fl);
                try {
                    ResultScanner scanner = table.getScanner(scan);
                    Result scannerResult;
                    while((scannerResult = scanner.next()) != null) {
                        byte[] sourceId = scannerResult.getValue(Bytes.toBytes("cf"), Bytes.toBytes("id"));
                        put = new Put(Bytes.toBytes(UUID.randomUUID().toString()));
                        put.add(Bytes.toBytes("cf"), Bytes.toBytes("source"), sourceId);
                        put.add(Bytes.toBytes("cf"), Bytes.toBytes("target"), Bytes.toBytes(id));
                        put.add(Bytes.toBytes("cf"), Bytes.toBytes("label"), Bytes.toBytes(predicate.toString()));
                        put.add(Bytes.toBytes("cf"), Bytes.toBytes("type"), Bytes.toBytes("edge"));
                        table.put(put);
                    }
                    scanner.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

                id++;
            }
            offset++;
            jedis.set(String.format("rdf2subdue:%s:offset", dataset), String.valueOf(offset));
        }
        try {
            table.close();
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        logger.info("Loading Done!");
    }
}
