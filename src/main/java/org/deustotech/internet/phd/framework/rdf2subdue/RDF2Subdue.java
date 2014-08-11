package org.deustotech.internet.phd.framework.rdf2subdue;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.vocabulary.RDF;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.client.utils.URIBuilder;
import org.apache.thrift.TException;
import org.hypertable.thrift.ThriftClient;
import org.hypertable.thriftgen.*;
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


    private static int LIMIT = 1000;

    public static void run(String dataset, String outputDir, boolean cont) {
        if (!cont) {
            generateId(dataset);
        }
        writeFile(dataset, outputDir);
    }

    private static void writeFile(String dataset, String outputDir) {
        Logger logger = Logger.getLogger(RDF2Subdue.class.getName());

        ThriftClient client = null;

        try {
            client = ThriftClient.create("localhost", 15867);
        } catch (TException e) {
            System.out.println(e);
            System.exit(1);
        }

        long ns = 0;
        try {
            ns = client.namespace_open("rdf2subdue");
        } catch (TException e) {
            e.printStackTrace();
        }

        logger.info("Writing files...");
        String dir = String.format("%s/%s", outputDir, dataset);
        new File(dir).mkdir();
        logger.info("Counting vertices...");

        HqlResult hqlVertexResult = null;
        try {
            hqlVertexResult = client.hql_query(ns, String.format("SELECT * from %s WHERE type = 'vertex'", dataset));
        } catch (TException e) {
            e.printStackTrace();
        }

        HqlResult hqlEdgeResult = null;
        try {
            hqlEdgeResult = client.hql_query(ns, String.format("SELECT * from %s WHERE type = 'edge'", dataset));
        } catch (TException e) {
            e.printStackTrace();
        }

        long total = 0;
        for (Cell cell : hqlVertexResult.getCells()) {
            total++;
        }

        boolean end = false;
        int limit = 1000;
        long lowerLimit = 0;
        long upperLimit = 1000;
        int count = 1;


        while (!end) {
            File f = new File(String.format("%s/%s_%s.g", dir, dataset, count));
            if (!f.exists()) {
                logger.info(String.format("Writing %s_%s.g...", dataset, count));

                SortedMap<Long, String> orderedVertices = new TreeMap<>();

                File file = new File(String.format("%s/%s_%s.g", dir, dataset, count));
                FileWriter fw = null;
                try {
                    fw = new FileWriter(file.getAbsoluteFile());
                } catch (IOException e) {
                    e.printStackTrace();
                }
                BufferedWriter bw = new BufferedWriter(fw);

                for (Cell cell : hqlVertexResult.getCells()) {
                    try {
                        List<Cell> results = client.get_row(ns, dataset, cell.getKey().getRow());
                        long id = 0;
                        String label = null;
                        for (Cell result : results) {
                            Key key = result.getKey();
                            if (key.getColumn_family().equals("id")) {
                                id = Long.parseLong(Bytes.toString(result.getValue()));
                            } else if (key.getColumn_family().equals("label")) {
                                label = Bytes.toString(result.getValue());
                            }
                        }
                        if (id <= upperLimit && id > lowerLimit) {
                            orderedVertices.put(id, label);
                        }
                    } catch (TException e) {
                        e.printStackTrace();
                    }

                }

                Set<Long> ids = orderedVertices.keySet();
                for (long id : ids) {
                    String label = orderedVertices.get(id);
                    try {
                        bw.write(String.format("v %s %s\n", id, label));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

                try {
                    bw.flush();
                } catch (IOException e) {
                    e.printStackTrace();
                }

                for (Cell cell : hqlEdgeResult.getCells()) {
                    try {
                        List<Cell> results = client.get_row(ns, dataset, cell.getKey().getRow());
                        long source = 0;
                        long target = 0;
                        String label = null;

                        for (Cell result : results) {
                            Key key = result.getKey();
                            if (key.getColumn_family().equals("source")) {
                                source = Long.parseLong(Bytes.toString(result.getValue()));
                            } else if (key.getColumn_family().equals("target")) {
                                target = Long.parseLong(Bytes.toString(result.getValue()));
                            } else if (key.getColumn_family().equals("label")) {
                                label = Bytes.toString(result.getValue());
                            }
                        }

                        if ((source <= upperLimit && target <= upperLimit) && (source > lowerLimit || target > lowerLimit)) {
                            try {
                                bw.write(String.format("d %s %s %s\n", source, target, label));
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }

                    } catch (TException e) {
                        e.printStackTrace();
                    }
                }

                try {
                    bw.flush();
                    bw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            } else {
                logger.info(String.format("Skipping %s_%s.g", dataset, count));
            }
            lowerLimit += limit;
            upperLimit += limit;
            count++;
            if (lowerLimit > total) {
                end = true;
            }
        }


        logger.info("End!");


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


        ThriftClient client = null;

        try {
            client = ThriftClient.create("localhost", 15867);
        } catch (TException e) {
            System.out.println(e);
            System.exit(1);
        }

        long ns = 0;
        try {
            if (!client.namespace_exists("rdf2subdue")) {
                client.namespace_create("rdf2subdue");
            }
            ns = client.namespace_open("rdf2subdue");
        } catch (TException e) {
            e.printStackTrace();
        }

        Schema schema = new Schema();

        Map columnFamilies = new HashMap();
        ColumnFamilySpec cf = new ColumnFamilySpec();
        cf.setName("id");
        columnFamilies.put("id", cf);

        cf = new ColumnFamilySpec();
        cf.setName("label");
        columnFamilies.put("label", cf);

        cf = new ColumnFamilySpec();
        cf.setName("type");
        columnFamilies.put("type", cf);

        cf = new ColumnFamilySpec();
        cf.setName("source");
        columnFamilies.put("source", cf);

        cf = new ColumnFamilySpec();
        cf.setName("target");
        columnFamilies.put("target", cf);

        schema.setColumn_families(columnFamilies);

        try {
            client.table_create(ns, dataset, schema);
        } catch (TException e) {
            e.printStackTrace();
        }

        long offsetQ1 = 0;
        Jedis jedis = new Jedis("localhost");
        /*if (jedis.exists(String.format("rdf2subdue:%s:offset", dataset))) {
            offsetQ1 = Long.parseLong(jedis.get((String.format("rdf2subdue:%s:offset", dataset))));
        }*/

        VirtGraph graph = new VirtGraph("http://" + dataset, connectionURL.toString(), prop.getProperty("virtuoso_user"), prop.getProperty("virtuoso_password"));
        /* Query query = QueryFactory.create(String.format("SELECT DISTINCT ?s ?class WHERE {?s a ?class} ORDER BY ?S OFFSET %s LIMIT %s", offsetQ1, LIMIT));
        VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(query, graph);
        ResultSet results = vqe.execSelect(); */
        logger.info("Generating vertices...");

        long id = 1;
        long count = 0;
        List cells = new ArrayList();
        boolean end = false;
        while (!end) {
            Query query = QueryFactory.create(String.format("SELECT DISTINCT ?s ?class WHERE {?s a ?class} OFFSET %s LIMIT %s", offsetQ1, LIMIT));
            VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(query, graph);
            ResultSet results = vqe.execSelect();

            while (results.hasNext()) {
                QuerySolution result = results.next();
                String subject = result.getResource("s").getURI();
                String clazz = result.getResource("class").getURI();

                jedis.lpush(String.format("rdf2subdue:%s:vertex:%s", dataset, subject), String.valueOf(id));

                Key key = null;
                Cell cell = null;

                String keyId = UUID.randomUUID().toString();

                key = new Key();
                key.setRow(keyId);
                key.setColumn_family("id");
                cell = new Cell();
                cell.setKey(key);

                try {
                    cell.setValue(String.valueOf(id).getBytes("UTF-8"));
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }

                cells.add(cell);

                key = new Key();
                key.setRow(keyId);
                key.setColumn_family("label");
                cell = new Cell();
                cell.setKey(key);
                try {
                    cell.setValue(String.format("<%s>", clazz).getBytes("UTF-8"));
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
                cells.add(cell);

                key = new Key();
                key.setRow(keyId);
                key.setColumn_family("type");
                cell = new Cell();
                cell.setKey(key);
                try {
                    cell.setValue("vertex".getBytes("UTF-8"));
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
                cells.add(cell);

                id++;


                //jedis.set(String.format("rdf2subdue:%s:offset", dataset), String.valueOf(offset));

                count++;
                if (count >= 200000) {
                    try {
                        client.set_cells(ns, dataset, cells);
                        cells = new ArrayList();
                        count = 0;
                    } catch (TException e) {
                        e.printStackTrace();
                    }
                }
            }

            if (results.getRowNumber() <= 0) {
                end = true;
            }

            vqe.close();
            offsetQ1 += LIMIT;

        }

        try {
            client.set_cells(ns, dataset, cells);
        } catch (TException e) {
            e.printStackTrace();
        }


        end = false;

        count = 0;
        cells = new ArrayList();
        int offsetQ2 = 0;
        while (!end) {

            Query query = QueryFactory.create(String.format("SELECT DISTINCT ?o WHERE { ?s ?p ?o . FILTER EXISTS { ?s a ?class } . FILTER NOT EXISTS { ?o ?p2 ?o2 } } OFFSET %s LIMIT %s", offsetQ2, LIMIT));
            VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(query, graph);
            ResultSet results = vqe.execSelect();

            long literalHash = 0;
            while (results.hasNext()) {
                QuerySolution result = results.next();
                String object = result.get("o").toString();

                if (jedis.exists(String.format("rdf2subdue:%s:vertex:%s", dataset, object))) {
                    List<Long> idList = new ArrayList<>();
                    idList.add(id);
                    for (Long item : idList) {
                        jedis.lpush(String.format("rdf2subdue:%s:vertex:%s", dataset, object), String.valueOf(item));
                    }

                    Key key = null;
                    Cell cell = null;

                    String keyId = UUID.randomUUID().toString();

                    key = new Key();
                    key.setRow(keyId);
                    key.setColumn_family("id");
                    cell = new Cell();
                    cell.setKey(key);

                    try {
                        cell.setValue(String.valueOf(id).getBytes("UTF-8"));
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }

                    cells.add(cell);

                    if (result.get("o").isLiteral()) {
                        //object = String.format("\"%s\"", object);
                        object = String.valueOf(literalHash);
                        literalHash++;
                    } else {
                        object = String.format("<%s>", object);
                    }

                    key = new Key();
                    key.setRow(keyId);
                    key.setColumn_family("label");
                    cell = new Cell();
                    cell.setKey(key);
                    try {
                        cell.setValue(object.getBytes("UTF-8"));
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                    cells.add(cell);


                    key = new Key();
                    key.setRow(keyId);
                    key.setColumn_family("type");
                    cell = new Cell();
                    cell.setKey(key);
                    try {
                        cell.setValue("vertex".getBytes("UTF-8"));
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                    cells.add(cell);

                    id++;

                    count++;
                    if (count >= 200000) {
                        try {
                            client.set_cells(ns, dataset, cells);
                            cells = new ArrayList();
                            count = 0;
                        } catch (TException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }

            if (results.getRowNumber() <= 0) {
                end = true;
            }

            offsetQ2 += LIMIT;
            vqe.close();
        }

        try {
            client.set_cells(ns, dataset, cells);
        } catch (TException e) {
            e.printStackTrace();
        }


        end = false;
        count = 0;
        cells = new ArrayList();
        int offsetQ3 = 0;

        while (!end) {

            Query query = QueryFactory.create(String.format("SELECT DISTINCT ?s ?p ?o WHERE { ?s ?p ?o . FILTER EXISTS { ?s a ?class } } OFFSET %s LIMIT %s", offsetQ3, LIMIT));
            VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(query, graph);
            ResultSet results = vqe.execSelect();

            while (results.hasNext()) {
                QuerySolution result = results.next();
                List<String> sourceIdList = jedis.lrange(String.format("rdf2subdue:%s:vertex:%s", dataset, result.get("s").toString()), 0, jedis.llen(String.format("rdf2subdue:%s:vertex:%s", dataset, result.get("s").toString())));
                String predicate = result.get("p").toString();
                if (!predicate.equals(RDF.type.getURI())) {
                    if (sourceIdList != null) {
                        for (String sourceId : sourceIdList) {
                            List<String> targetIdList = jedis.lrange(String.format("rdf2subdue:%s:vertex:%s", dataset, result.get("o").toString()), 0, jedis.llen(String.format("rdf2subdue:%s:vertex:%s", dataset, result.get("o").toString())));
                            if (targetIdList != null) {
                                for (String targetId : targetIdList) {
                                    Key key = null;
                                    Cell cell = null;

                                    String keyId = UUID.randomUUID().toString();

                                    key = new Key();
                                    key.setRow(keyId);
                                    key.setColumn_family("source");
                                    cell = new Cell();
                                    cell.setKey(key);
                                    try {
                                        cell.setValue(String.valueOf(sourceId).getBytes("UTF-8"));
                                    } catch (UnsupportedEncodingException e) {
                                        e.printStackTrace();
                                    }
                                    cells.add(cell);

                                    key = new Key();
                                    key.setRow(keyId);
                                    key.setColumn_family("target");
                                    cell = new Cell();
                                    cell.setKey(key);
                                    try {
                                        cell.setValue(String.valueOf(targetId).getBytes("UTF-8"));
                                    } catch (UnsupportedEncodingException e) {
                                        e.printStackTrace();
                                    }
                                    cells.add(cell);

                                    key = new Key();
                                    key.setRow(keyId);
                                    key.setColumn_family("label");
                                    cell = new Cell();
                                    cell.setKey(key);
                                    try {
                                        cell.setValue(String.format("<%s>", predicate).getBytes("UTF-8"));
                                    } catch (UnsupportedEncodingException e) {
                                        e.printStackTrace();
                                    }
                                    cells.add(cell);

                                    key = new Key();
                                    key.setRow(keyId);
                                    key.setColumn_family("type");
                                    cell = new Cell();
                                    cell.setKey(key);
                                    try {
                                        cell.setValue("edge".getBytes("UTF-8"));
                                    } catch (UnsupportedEncodingException e) {
                                        e.printStackTrace();
                                    }
                                    cells.add(cell);

                                    count++;
                                    if (count >= 200000) {
                                        try {
                                            client.set_cells(ns, dataset, cells);
                                            cells = new ArrayList();
                                            count = 0;
                                        } catch (TException e) {
                                            e.printStackTrace();
                                        }
                                    }

                                }
                            }
                        }
                    }
                }
            }

            int rowNumber = results.getRowNumber();

            if (results.getRowNumber() <= 0) {
                end = true;
            }

            offsetQ3 += LIMIT;
            vqe.close();
        }

        try {
            client.set_cells(ns, dataset, cells);
        } catch (TException e) {
            e.printStackTrace();
        }

        try {
            client.namespace_close(ns);
            client.close();
        } catch (TException e) {
            e.printStackTrace();
        }

        logger.info("Loading Done!");
    }
}
