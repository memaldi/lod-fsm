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
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.client.utils.URIBuilder;
import virtuoso.jena.driver.VirtGraph;
import virtuoso.jena.driver.VirtuosoQueryExecution;
import virtuoso.jena.driver.VirtuosoQueryExecutionFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.logging.Logger;

/**
 * Created by memaldi on 14/06/14.
 */
public class RDF2Subdue {

    public void launch(String dataset) {
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

        VirtGraph graph = new VirtGraph("http://" + dataset, connectionURL.toString(), prop.getProperty("virtuoso_user"), prop.getProperty("virtuoso_password"));
        Query query = QueryFactory.create("SELECT DISTINCT ?s ?p ?o ?class WHERE { ?s ?p ?o OPTIONAL { ?o a ?class } }");
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
                FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL);
                SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("uri"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(object.toString()));
                filter.setFilterIfMissing(true);
                fl.addFilter(filter);
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
                    FilterList fl2 = new FilterList(FilterList.Operator.MUST_PASS_ALL);
                    SingleColumnValueFilter filter2 = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("uri"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(subject.toString()));
                    filter2.setFilterIfMissing(true);
                    fl2.addFilter(filter2);
                    Scan scan2 = new Scan();
                    scan.setFilter(fl2);
                    ResultScanner scanner2 = table.getScanner(scan2);
                    Result scannerResult2;

                    while((scannerResult2 = scanner2.next()) != null) {
                        sourceId = scannerResult2.getValue(Bytes.toBytes("cf"), Bytes.toBytes("id"));
                    }
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

                FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL);
                SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("uri"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(subject.toString()));
                filter.setFilterIfMissing(true);
                fl.addFilter(filter);
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


        }
        try {
            table.close();
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        logger.info("Done!");
    }
}
