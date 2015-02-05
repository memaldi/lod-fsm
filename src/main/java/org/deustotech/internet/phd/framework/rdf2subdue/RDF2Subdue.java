package org.deustotech.internet.phd.framework.rdf2subdue;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.vocabulary.RDF;
import org.apache.http.client.utils.URIBuilder;
import org.apache.thrift.TException;
import org.hypertable.thrift.ThriftClient;
import org.hypertable.thriftgen.*;
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
    private static int FLUSH_LIMIT = 20000;
    private static String THRIFT_SERVER = "helheim.deusto.es";

    public static void run(String dataset, String outputDir, boolean cont, boolean literals) {
        if (!cont) {
            generateId(dataset, literals);
        }
        writeFile(dataset, outputDir);
    }

    private static void writeFile(String dataset, String outputDir) {
        Logger logger = Logger.getLogger(RDF2Subdue.class.getName());

        ThriftClient client = null;

        try {
            client = ThriftClient.create(THRIFT_SERVER, 15867, 1600000, true, 400 * 1024 * 1024);
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

        boolean end = false;
        long lowerLimit = 1;
        long upperLimit = 1000;
        int count = 1;


        while (!end) {
            File f = new File(String.format("%s/%s_%s.g", dir, dataset, count));
            if (!f.exists()) {


                String query = String.format("SELECT type FROM %s WHERE '%s' <= ROW < '%s' AND type = 'vertex' KEYS_ONLY", dataset, getPaddedID(lowerLimit), getPaddedID(upperLimit));
                try {
                    HqlResult hqlResult = client.hql_query(ns, query);
                    if (hqlResult.getCells().size() > 0) {
                        logger.info(String.format("Writing %s_%s.g...", dataset, count));

                        File file = new File(String.format("%s/%s_%s.g", dir, dataset, count));
                        FileWriter fw = null;
                        try {
                            fw = new FileWriter(file.getAbsoluteFile());
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        BufferedWriter bw = new BufferedWriter(fw);
                        for (Cell cell : hqlResult.getCells()) {
                            ByteBuffer labelBuffer = client.get_cell(ns, dataset.replace("-", "_"), cell.getKey().getRow(), "label");
                            String label = new String(labelBuffer.array(), labelBuffer.position(), labelBuffer.remaining());
                            bw.write(String.format("v %s %s\n", getDepaddedId(cell.getKey().getRow()), label));
                        }
                        bw.flush();

                        query = String.format("SELECT type FROM %s WHERE type = 'edge' and source =^ '%s' or target =^ '%s' KEYS_ONLY", dataset, paddedLimit(upperLimit), paddedLimit(upperLimit));

                        HqlResult edgeHqlResult = client.hql_query(ns, query);
                        if (edgeHqlResult.getCells().size() > 0) {
                            Set<String> rowSet = new HashSet<>();
                            for (Cell cell : edgeHqlResult.getCells()) {
                                rowSet.add(cell.getKey().getRow());
                            }
                            for (String row : rowSet) {
                                ByteBuffer sourceBuffer = client.get_cell(ns, dataset.replace("-", "_"), row, "source");
                                long source = Long.parseLong(getDepaddedId(new String(sourceBuffer.array(), sourceBuffer.position(), sourceBuffer.remaining())));
                                ByteBuffer targetBuffer = client.get_cell(ns, dataset.replace("-", "_"), row, "target");
                                long target = Long.parseLong(getDepaddedId(new String(targetBuffer.array(), targetBuffer.position(), targetBuffer.remaining())));

                                if ((source < upperLimit && target < upperLimit) && (source >= lowerLimit || target >= lowerLimit)) {
                                    ByteBuffer labelBuffer = client.get_cell(ns, dataset.replace("-", "_"), row, "label");
                                    String label = new String(labelBuffer.array(), labelBuffer.position(), labelBuffer.remaining());
                                    bw.write(String.format("d %s %s %s\n", source, target, label));
                                }
                            }
                        }

                        bw.close();
                    } else {
                        end = true;
                    }

                } catch (TException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {
                logger.info(String.format("Skipping %s_%s.g", dataset, count));
            }
            lowerLimit = upperLimit;
            upperLimit += LIMIT;
            count++;
        }

        logger.info("End!");

    }

    private static String paddedLimit(long limit) {
        String origStrLimit = String.valueOf(limit);
        String strLimit = String.valueOf(Integer.valueOf(String.valueOf(origStrLimit.charAt(0))) - 1);

        String paddedZeros = "";
        for (int i = 0; i <= 10-String.valueOf(origStrLimit).length(); i++) {
            paddedZeros += "0";
        }

        return paddedZeros + strLimit;
    }

    private static String getDepaddedId(String id) {
        return id.replaceFirst("^0+(?!$)", "");
    }

    private static void generateId(String dataset, boolean literals) {
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
            client = ThriftClient.create(THRIFT_SERVER, 15867);
        } catch (TException e) {
            e.printStackTrace();
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

        createTable(schema, client , ns, dataset);

        VirtGraph graph = new VirtGraph("http://" + dataset, connectionURL.toString(), prop.getProperty("virtuoso_user"), prop.getProperty("virtuoso_password"));

        logger.info("Generating vertices...");

        long literalHash = 0;
        long id = 1;
        long offset = 0;
        int flush = 0;

        List<Cell> cellList = new ArrayList<>();
        // Subjects
        while(true) {
            Query sparqlQuery = QueryFactory.create(String.format("SELECT DISTINCT ?s ?class WHERE {?s a ?class .} OFFSET %s LIMIT %s", offset, LIMIT));
            VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(sparqlQuery, graph);
            ResultSet results = vqe.execSelect();

            while (results.hasNext()) {
                QuerySolution next = results.next();
                String subject = next.getResource("s").getURI();
                if (subject != null) {
                    String clazz = next.getResource("class").getURI();

                    cellList.addAll(insertVertex(id, subject, clazz));
                    id++;
                    flush++;
                }
            }
            if (flush >= FLUSH_LIMIT) {
                try {
                    client.set_cells(ns, dataset.replace("-", "_"), cellList);
                    cellList = new ArrayList();
                    flush = 0;
                    System.gc();
                } catch (TException e) {
                    e.printStackTrace();
                }
            }

            if (results.getRowNumber() <= 0) {
                break;
            }
            offset += LIMIT;
        }

        try {
            client.set_cells(ns, dataset.replace("-", "_"), cellList);
        } catch (TException e) {
            e.printStackTrace();
        }

        logger.info("Generating edges...");
        //Edges
        offset = 0;
        flush = 0;
        cellList = new ArrayList<>();
        System.gc();
        while(true) {
            Query sparqlQuery = QueryFactory.create(String.format("SELECT DISTINCT ?s ?p ?o WHERE { ?s a ?class . ?s ?p ?o . } OFFSET %s LIMIT %s", offset, LIMIT));
            VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(sparqlQuery, graph);
            ResultSet results = vqe.execSelect();

            while (results.hasNext()) {
                QuerySolution next = results.next();
                String subject = next.getResource("s").getURI();
                String predicate = next.getResource("p").getURI();
                List<String> targetIDList = new ArrayList<>();
                if (subject != null && !predicate.equals(RDF.type.getURI())) {

                    if (next.get("o").isLiteral()) {
                        String object = next.getLiteral("o").getString();
                        if (literals) {
                            cellList.addAll(insertVertex(id, object, String.valueOf(literalHash)));
                            literalHash++;
                        } else {
                            cellList.addAll(insertVertex(id, object, "LITERAL"));
                        }
                        String targetID = getPaddedID(id);
                        id++;
                        flush++;
                        targetIDList.add(targetID);
                    } else if (next.get("o").isURIResource()) {
                        String object = next.getResource("o").getURI();
                        String hqlQuery = String.format("SELECT type FROM %s WHERE source = \"%s\" KEYS_ONLY", dataset, object);
                        try {
                            HqlResult hqlResult = client.hql_query(ns, hqlQuery);
                            if (hqlResult.getCells().size() > 0) {
                                for (Cell cell : hqlResult.getCells()) {
                                    //ByteBuffer labelBuffer = client.get_cell(ns, dataset.replace("-", "_"), cell.getKey().getRow(), "id");
                                    //String targetID = new String(labelBuffer.array(), labelBuffer.position(), labelBuffer.remaining());
                                    String targetID = cell.getKey().getRow();
                                    targetIDList.add(targetID);
                                }
                            }
                        } catch (TException e) {
                            e.printStackTrace();
                        }
                    }
                }

                if (targetIDList.size() > 0) {
                    String hqlQuery = String.format("SELECT type FROM %s WHERE source = \"%s\" KEYS_ONLY", dataset, subject);
                    try {
                        HqlResult hqlResult = client.hql_query(ns, hqlQuery);
                        if (hqlResult.getCells().size() > 0) {
                            for (Cell cell : hqlResult.getCells()) {
                                //ByteBuffer labelBuffer = client.get_cell(ns, dataset.replace("-", "_"), cell.getKey().getRow(), "id");
                                //String sourceID = new String(labelBuffer.array(), labelBuffer.position(), labelBuffer.remaining());
                                String sourceID = cell.getKey().getRow();
                                for (String targetID : targetIDList) {
                                    cellList.addAll(insertEdge(predicate, sourceID, targetID));
                                }
                            }
                        }
                    } catch (TException e) {
                        e.printStackTrace();
                    }
                }
            }
            if (flush >= FLUSH_LIMIT) {
                try {
                    client.set_cells(ns, dataset.replace("-", "_"), cellList);
                    cellList = new ArrayList();
                    System.gc();
                    flush = 0;
                } catch (TException e) {
                    e.printStackTrace();
                }
            }

            if (results.getRowNumber() <= 0) {
                break;
            }
            offset += LIMIT;
        }

        try {
            client.set_cells(ns, dataset.replace("-", "_"), cellList);
            System.gc();
        } catch (TException e) {
            e.printStackTrace();
        }
        logger.info("end");
    }

    private static String getPaddedID(long id) {
        String paddedZeros = "";
        for (int i = 0; i <= 10-String.valueOf(id).length(); i++) {
            paddedZeros += "0";
        }

        return paddedZeros + String.valueOf(id);
    }

    private static void createTable(Schema schema, ThriftClient client, long ns, String dataset) {
        Map columnFamilies = new HashMap();
        ColumnFamilySpec cf = new ColumnFamilySpec();
        /*cf.setName("id");
        cf.setValue_index(true);
        columnFamilies.put("id", cf);*/

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
    }



    private static List<Cell> insertEdge(String predicate, String source, String target) {
        Key key = null;
        Cell cell = null;

        List<Cell> cells = new ArrayList<>();

        //String keyId = getPaddedEdge(source, target, predicate);
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
        return cells;
    }

    private static String getPaddedEdge(String source, String target, String label) {

        long max = Math.max(Long.parseLong(source), Long.parseLong(target));
        long min = Math.min(Long.parseLong(source), Long.parseLong(target));

        String paddedMax = "";
        for (int i = 0; i <= 10 - String.valueOf(max).length() - 1; i++) {
            paddedMax += "0";
        }
        paddedMax += String.valueOf(max);

        String paddedMin = "";
        for (int i = 0; i <= 10 - String.valueOf(min).length() - 1; i++) {
            paddedMin += "0";
        }
        paddedMin += String.valueOf(min);
        return String.format("%s-%s-%s", paddedMax, paddedMin, label);
    }

    private static List<Cell> insertVertex(long id, String source, String label) {
        Key key = null;
        Cell cell = null;
        List<Cell> cells = new ArrayList<>();

        String keyId = getPaddedID(id);

        key = new Key();
        key.setRow(keyId);
        key.setColumn_family("label");
        cell = new Cell();
        cell.setKey(key);
        try {
            cell.setValue(String.format("<%s>", label).getBytes("UTF-8"));
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
            cell.setValue(source.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        cells.add(cell);

        return cells;
    }
}
