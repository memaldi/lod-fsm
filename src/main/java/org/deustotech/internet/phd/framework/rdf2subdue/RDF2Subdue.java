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
import java.nio.ByteBuffer;
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
        //writeFile(dataset, outputDir);
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

        boolean end = false;
        int limit = 1000;
        long lowerLimit = 1;
        long upperLimit = 1000;
        int count = 1;


        while (!end) {
            File f = new File(String.format("%s/%s_%s.g", dir, dataset, count));
            if (!f.exists()) {
                logger.info(String.format("Writing %s_%s.g...", dataset, count));

                File file = new File(String.format("%s/%s_%s.g", dir, dataset, count));
                FileWriter fw = null;
                try {
                    fw = new FileWriter(file.getAbsoluteFile());
                } catch (IOException e) {
                    e.printStackTrace();
                }
                BufferedWriter bw = new BufferedWriter(fw);

                String query = String.format("SELECT id FROM %s WHERE id >= %s AND id < %s KEYS_ONLY", dataset, lowerLimit, upperLimit);
                try {
                    HqlResult hqlResult = client.hql_query(ns, query);
                    if (hqlResult.getCells().size() > 0) {
                        for (Cell cell : hqlResult.getCells()) {
                            ByteBuffer labelBuffer = client.get_cell(ns, dataset.replace("-", "_"), cell.getKey().getRow(), "label");
                            String label = new String(labelBuffer.array(), labelBuffer.position(), labelBuffer.remaining());
                            //bw.write(String.format("v %s %s\n", i, label));
                        }
                    }
                } catch (TException e) {
                    e.printStackTrace();
                }


                /*for (long i = lowerLimit; i <= upperLimit; i++) {
                    try {
                        String query = String.format("SELECT id from %s WHERE id = '%s'", dataset.replace("-", "_"), i);
                        HqlResult hqlResult = client.hql_query(ns, query);
                        if (hqlResult.getCells().size() > 0) {
                            for (Cell cell : hqlResult.getCells()) {
                                ByteBuffer labelBuffer = client.get_cell(ns, dataset.replace("-", "_"), cell.getKey().getRow(), "label");
                                String label = new String(labelBuffer.array(), labelBuffer.position(), labelBuffer.remaining());
                                bw.write(String.format("v %s %s\n", i, label));
                            }
                        } else {
                            end = true;
                            break;
                        }
                    } catch (TException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

                try {
                    bw.flush();
                } catch (IOException e) {
                    e.printStackTrace();
                }

                for (long i = lowerLimit; i <= upperLimit; i++) {
                    try {
                        String query = String.format("SELECT source from %s WHERE source = '%s'", dataset.replace("-", "_"), i);
                        HqlResult hqlResult = client.hql_query(ns, query);
                        for (Cell cell : hqlResult.getCells()) {
                            ByteBuffer targetBuffer = client.get_cell(ns, dataset.replace("-", "_"), cell.getKey().getRow(), "target");
                            String target = new String(targetBuffer.array(), targetBuffer.position(), targetBuffer.remaining());
                            if (Long.parseLong(target) <= upperLimit) {
                                ByteBuffer labelBuffer = client.get_cell(ns, dataset.replace("-", "_"), cell.getKey().getRow(), "label");
                                String label = new String(labelBuffer.array(), labelBuffer.position(), labelBuffer.remaining());
                                bw.write(String.format("d %s %s %s\n", i, target, label));
                            }
                        }

                        query = String.format("SELECT target from %s WHERE target = '%s'", dataset.replace("-", "_"), i);
                        hqlResult = client.hql_query(ns, query);
                        for (Cell cell : hqlResult.getCells()) {
                            ByteBuffer sourceBuffer = client.get_cell(ns, dataset.replace("-", "_"), cell.getKey().getRow(), "source");
                            String source = new String(sourceBuffer.array(), sourceBuffer.position(), sourceBuffer.remaining());
                            if (Long.parseLong(source) < lowerLimit) {
                                ByteBuffer labelBuffer = client.get_cell(ns, dataset.replace("-", "_"), cell.getKey().getRow(), "label");
                                String label = new String(labelBuffer.array(), labelBuffer.position(), labelBuffer.remaining());
                                bw.write(String.format("d %s %s %s\n", source, i, label));
                            }
                        }
                    } catch (TException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

                try {
                    bw.flush();
                    bw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }*/

            } else {
                logger.info(String.format("Skipping %s_%s.g", dataset, count));
            }
            lowerLimit += limit;
            upperLimit += limit;
            count++;
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
        cf.setValue_index(true);
        columnFamilies.put("id", cf);

        cf = new ColumnFamilySpec();
        cf.setName("label");
        cf.setValue_index(true);
        columnFamilies.put("label", cf);

        cf = new ColumnFamilySpec();
        cf.setName("type");
        cf.setValue_index(true);
        columnFamilies.put("type", cf);

        cf = new ColumnFamilySpec();
        cf.setName("source");
        cf.setValue_index(true);
        columnFamilies.put("source", cf);

        cf = new ColumnFamilySpec();
        cf.setName("target");
        columnFamilies.put("target", cf);

        schema.setColumn_families(columnFamilies);

        try {
            client.table_create(ns, dataset.replace("-", "_"), schema);
        } catch (TException e) {
            e.printStackTrace();
        }

        VirtGraph graph = new VirtGraph("http://" + dataset, connectionURL.toString(), prop.getProperty("virtuoso_user"), prop.getProperty("virtuoso_password"));

        logger.info("Generating vertices...");

        long id = 1;
        long offset = 0;

        //OFFSET AND LIMIT
        while(true) {
            List cells = new ArrayList();
            Query sparqlQuery = QueryFactory.create(String.format("SELECT DISTINCT ?s ?p ?o WHERE {?s a ?class . ?s ?p ?o } OFFSET %s LIMIT %s", offset, LIMIT));
            VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(sparqlQuery, graph);
            ResultSet results = vqe.execSelect();
            while (results.hasNext()) {
                QuerySolution result = results.next();
                String subject = result.getResource("s").getURI();
                String predicate = result.getResource("p").getURI();
                String object;
                if (result.get("o").isLiteral()) {
                    object = result.getLiteral("o").getString();
                } else {
                    object = result.getResource("o").getURI();
                }

                if (predicate.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
                    String hQuery = String.format("SELECT id FROM %s WHERE source = '%s' AND label = '%s'", dataset, subject, object);
                    try {
                        HqlResult hqlResult = client.hql_query(ns, hQuery);
                        if (hqlResult.getCells().size() <= 0) {
                            // No existe
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
                                cell.setValue(String.format("<%s>", object).getBytes("UTF-8"));
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

                            key = new Key();
                            key.setRow(keyId);
                            key.setColumn_family("source");
                            cell = new Cell();
                            cell.setKey(key);
                            try {
                                cell.setValue(subject.getBytes("UTF-8"));
                            } catch (UnsupportedEncodingException e) {
                                e.printStackTrace();
                            }
                            cells.add(cell);


                            id++;
                            client.set_cells(ns, dataset.replace("-", "_"), cells);
                            cells = new ArrayList();
                        }
                    } catch (TException e) {
                        e.printStackTrace();
                    }

                } else {
                    if (result.get("o").isLiteral()) {
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
                            cell.setValue("LITERAL".getBytes("UTF-8"));
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

                        key = new Key();
                        key.setRow(keyId);
                        key.setColumn_family("source");
                        cell = new Cell();
                        cell.setKey(key);
                        try {
                            cell.setValue(object.replace("'", "\'").getBytes("UTF-8"));
                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                        }
                        cells.add(cell);

                        id++;
                        try {
                            client.set_cells(ns, dataset.replace("-", "_"), cells);
                        } catch (TException e) {
                            e.printStackTrace();
                        }
                        cells = new ArrayList();
                    } else {
                        String hQuery = String.format("SELECT id FROM %s WHERE source = '%s'", dataset, object);
                        try {
                            HqlResult hqlResult = client.hql_query(ns, hQuery);
                            if (hqlResult.getCells().size() <= 0) {

                                Query classQuery = QueryFactory.create(String.format("SELECT DISTINCT ?class WHERE {<%s> a ?class}", object));
                                VirtuosoQueryExecution classVqe = VirtuosoQueryExecutionFactory.create(classQuery, graph);
                                ResultSet classResults = classVqe.execSelect();

                                while (classResults.hasNext()) {

                                    QuerySolution classSolution = classResults.next();
                                    String clazz = classSolution.getResource("class").getURI();

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
                                        cell.setValue(clazz.getBytes("UTF-8"));
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

                                    key = new Key();
                                    key.setRow(keyId);
                                    key.setColumn_family("source");
                                    cell = new Cell();
                                    cell.setKey(key);
                                    try {
                                        cell.setValue(object.getBytes("UTF-8"));
                                    } catch (UnsupportedEncodingException e) {
                                        e.printStackTrace();
                                    }
                                    cells.add(cell);

                                    id++;
                                    try {
                                        client.set_cells(ns, dataset.replace("-", "_"), cells);
                                    } catch (TException e) {
                                        e.printStackTrace();
                                    }
                                    cells = new ArrayList();
                                }
                            }
                        } catch (TException e) {
                            e.printStackTrace();
                        }
                    }

                    // Edge
                    List<String> sourceIDList = new ArrayList<>();
                    List<String> targetIDList = new ArrayList<>();

                    String sourceQuery = String.format("SELECT id FROM %s WHERE source = '%s' KEYS_ONLY", dataset, subject);
                    String targetQuery = String.format("SELECT id FROM %s WHERE source = '%s' KEYS_ONLY", dataset, object.replace("'", "\'"));

                    try {
                        HqlResult hqlResult = client.hql_query(ns, sourceQuery);
                        for (Cell cell : hqlResult.getCells()) {
                            ByteBuffer labelBuffer = client.get_cell(ns, dataset.replace("-", "_"), cell.getKey().getRow(), "id");
                            String stringID = new String(labelBuffer.array(), labelBuffer.position(), labelBuffer.remaining());
                            sourceIDList.add(stringID);
                        }
                    } catch (TException e) {
                        e.printStackTrace();
                    }

                    try {
                        HqlResult hqlResult = client.hql_query(ns, targetQuery);
                        for (Cell cell : hqlResult.getCells()) {
                            ByteBuffer labelBuffer = client.get_cell(ns, dataset.replace("-", "_"), cell.getKey().getRow(), "id");
                            String stringID = new String(labelBuffer.array(), labelBuffer.position(), labelBuffer.remaining());
                            targetIDList.add(stringID);
                        }
                    } catch (TException e) {
                        e.printStackTrace();
                    }

                    for (String source : sourceIDList) {
                        for (String target : targetIDList) {
                            Key key = null;
                            Cell cell = null;

                            String keyId = UUID.randomUUID().toString();

                            key = new Key();
                            key.setRow(keyId);
                            key.setColumn_family("source");
                            cell = new Cell();
                            cell.setKey(key);

                            try {
                                cell.setValue(String.valueOf(source).getBytes("UTF-8"));
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
                                cell.setValue(String.valueOf(target).getBytes("UTF-8"));
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
                                cell.setValue(String.valueOf(predicate).getBytes("UTF-8"));
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
                                cell.setValue(String.valueOf("edge").getBytes("UTF-8"));
                            } catch (UnsupportedEncodingException e) {
                                e.printStackTrace();
                            }

                            cells.add(cell);

                            try {
                                client.set_cells(ns, dataset.replace("-", "_"), cells);
                            } catch (TException e) {
                                e.printStackTrace();
                            }
                            cells = new ArrayList();
                        }
                    }
                }
            }
            if (results.getRowNumber() <= 0) {
                break;
            }
            offset += LIMIT;
        }
    }
}
