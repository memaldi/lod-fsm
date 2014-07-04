package org.deustotech.internet.phd.framework.rdf2subdue;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
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
import java.util.*;
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
        List<Filter> andFilterList = new ArrayList<>();
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("type"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes("vertex"));
        filter.setFilterIfMissing(true);
        andFilterList.add(filter);
        FilterList andFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL, andFilterList);
        Scan scan = new Scan();
        scan.setFilter(andFilter);

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
                andFilterList = new ArrayList<>();
                SingleColumnValueFilter lowerFilter = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("id"), CompareFilter.CompareOp.GREATER, Bytes.toBytes(lowerLimit));
                lowerFilter.setFilterIfMissing(true);
                SingleColumnValueFilter upperFilter = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("id"), CompareFilter.CompareOp.LESS_OR_EQUAL, Bytes.toBytes(upperLimit));
                upperFilter.setFilterIfMissing(true);
                andFilterList.add(lowerFilter);
                andFilterList.add(upperFilter);
                andFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL, andFilterList);
                scan = new Scan();
                scan.setFilter(andFilter);
                scanner = table.getScanner(scan);
                File file = new File(String.format("%s/%s_%s.g", dir, dataset, count));
                FileWriter fw = new FileWriter(file.getAbsoluteFile());
                BufferedWriter bw = new BufferedWriter(fw);
                SortedMap<Long, String> orderedVertices = new TreeMap<>();
                while ((scannerResult = scanner.next()) != null) {
                    long id = Bytes.toLong(scannerResult.getValue(Bytes.toBytes("cf"), Bytes.toBytes("id")));
                    String label = Bytes.toString(scannerResult.getValue(Bytes.toBytes("cf"), Bytes.toBytes("label")));
                    orderedVertices.put(id, label);
                    //bw.write(String.format("v %s %s\n", id, label));
                }

                Set<Long> ids = orderedVertices.keySet();
                for (long id : ids) {
                    String label = orderedVertices.get(id);
                    bw.write(String.format("v %s %s\n", id, label));
                }

                scanner.close();
                bw.flush();

                andFilterList = new ArrayList<>();
                SingleColumnValueFilter sourceLowerFilter = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("source"), CompareFilter.CompareOp.GREATER, Bytes.toBytes(lowerLimit));
                sourceLowerFilter.setFilterIfMissing(true);
                SingleColumnValueFilter sourceUpperFilter = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("source"), CompareFilter.CompareOp.LESS_OR_EQUAL, Bytes.toBytes(upperLimit));
                sourceUpperFilter.setFilterIfMissing(true);
                SingleColumnValueFilter targetLowerFilter = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("target"), CompareFilter.CompareOp.GREATER, Bytes.toBytes(lowerLimit));
                targetLowerFilter.setFilterIfMissing(true);
                SingleColumnValueFilter targetUpperFilter = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("target"), CompareFilter.CompareOp.LESS_OR_EQUAL, Bytes.toBytes(upperLimit));
                targetUpperFilter.setFilterIfMissing(true);

                //filterList.add(sourceLowerFilter);
                andFilterList.add(sourceUpperFilter);
                //filterList.add(targetLowerFilter);
                andFilterList.add(targetUpperFilter);

                andFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL, andFilterList);

                List<Filter> orFilterList = new ArrayList<>();
                orFilterList.add(sourceLowerFilter);
                orFilterList.add(targetLowerFilter);
                FilterList orFilter = new FilterList(FilterList.Operator.MUST_PASS_ONE, orFilterList);

                FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL);
                fl.addFilter(andFilter);
                fl.addFilter(orFilter);

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
            Query query = QueryFactory.create("SELECT DISTINCT ?s ?class WHERE {?s a ?class} ORDER BY ?s OFFSET" + offset);
        VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(query, graph);
        ResultSet results = vqe.execSelect();
        logger.info("Generating vertices...");

        Map<String, List<Long>> vertexMap = new HashMap<>();

        long id = 1;

        while(results.hasNext()) {
            QuerySolution result = results.next();
            String subject = result.getResource("s").getURI();
            String clazz = result.getResource("class").getURI();

            if (!vertexMap.containsKey(subject)) {
                vertexMap.put(subject, new ArrayList<Long>());
            }

            vertexMap.get(subject).add(id);

            Put put = new Put(Bytes.toBytes(UUID.randomUUID().toString()));
            put.add(Bytes.toBytes("cf"), Bytes.toBytes("id"), Bytes.toBytes(id));
            put.add(Bytes.toBytes("cf"), Bytes.toBytes("label"), Bytes.toBytes(String.format("<%s>", clazz)));
            put.add(Bytes.toBytes("cf"), Bytes.toBytes("type"), Bytes.toBytes("vertex"));

            try {
                table.put(put);
            } catch (InterruptedIOException e) {
                e.printStackTrace();
            } catch (RetriesExhaustedWithDetailsException e) {
                e.printStackTrace();
            }
            id++;

            offset++;
            jedis.set(String.format("rdf2subdue:%s:offset", dataset), String.valueOf(offset));
        }
        vqe.close();

        query = QueryFactory.create("SELECT DISTINCT ?o WHERE { ?s ?p ?o . FILTER EXISTS { ?s a ?class } . FILTER NOT EXISTS { ?o ?p2 ?o2 } } ORDER BY ?o");
        vqe = VirtuosoQueryExecutionFactory.create(query, graph);
        results = vqe.execSelect();

        while(results.hasNext()) {
            QuerySolution result = results.next();
            String object = result.get("o").toString();
            if (!vertexMap.containsKey(object)) {
                List<Long> idList = new ArrayList<>();
                idList.add(id);
                vertexMap.put(object, idList);
                Put put = new Put(Bytes.toBytes(UUID.randomUUID().toString()));
                put.add(Bytes.toBytes("cf"), Bytes.toBytes("id"), Bytes.toBytes(id));
                if (result.get("o").isLiteral()) {
                    object = String.format("\"%s\"", object);
                } else {
                    object = String.format("<%s>", object);
                }
                put.add(Bytes.toBytes("cf"), Bytes.toBytes("label"), Bytes.toBytes(object));
                put.add(Bytes.toBytes("cf"), Bytes.toBytes("type"), Bytes.toBytes("vertex"));

                try {
                    table.put(put);
                } catch (InterruptedIOException e) {
                    e.printStackTrace();
                } catch (RetriesExhaustedWithDetailsException e) {
                    e.printStackTrace();
                }
                id++;
            }
        }
        vqe.close();

        query = QueryFactory.create("SELECT DISTINCT ?s ?p ?o WHERE { ?s ?p ?o . FILTER EXISTS { ?s a ?class } } ORDER BY ?s");
        vqe = VirtuosoQueryExecutionFactory.create(query, graph);
        results = vqe.execSelect();

        while (results.hasNext()) {
            QuerySolution result = results.next();
            List<Long> sourceIdList = vertexMap.get(result.get("s").toString());
            String predicate = result.get("p").toString();
            if (!predicate.equals(RDF.type.getURI())) {
                if (sourceIdList != null) {
                    for (long sourceId : sourceIdList) {
                        List<Long> targetIdList = vertexMap.get(result.get("o").toString());
                        if (targetIdList != null) {
                            for (long targetId : targetIdList) {
                                Put put = new Put(Bytes.toBytes(UUID.randomUUID().toString()));
                                put.add(Bytes.toBytes("cf"), Bytes.toBytes("source"), Bytes.toBytes(sourceId));
                                put.add(Bytes.toBytes("cf"), Bytes.toBytes("target"), Bytes.toBytes(targetId));
                                put.add(Bytes.toBytes("cf"), Bytes.toBytes("label"), Bytes.toBytes(predicate));
                                put.add(Bytes.toBytes("cf"), Bytes.toBytes("type"), Bytes.toBytes("edge"));

                                try {
                                    table.put(put);
                                } catch (InterruptedIOException e) {
                                    e.printStackTrace();
                                } catch (RetriesExhaustedWithDetailsException e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                    }
                }
            }
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
