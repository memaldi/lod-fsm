package org.deustotech.internet.phd.validation;

import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.thrift.TException;
import org.hypertable.thrift.ThriftClient;
import org.hypertable.thriftgen.*;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.util.*;

/**
 * Created by mikel (m.emaldi at deusto dot es) on 26/08/14.
 */
public class LoadDataHubGS {

    private static String API_URL = "/api/3/action/package_show?id=";

    public static void run(String inputCSV) {

        InputStream input = LoadDataHubGS.class.getResourceAsStream("/config.properties");
        Properties prop = new Properties();
        try {
            prop.load(input);
            input.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


        ThriftClient client = null;
        try {
            client = ThriftClient.create("localhost", 15867);
        } catch (TException e) {
            System.exit(1);
        }

        long ns = 0;
        try {
            if (!client.namespace_exists("gs")) {
                client.namespace_create("gs");
            }
            ns = client.namespace_open("gs");
        } catch (TException e) {
            e.printStackTrace();
        }

        Schema schema = new Schema();

        Map columnFamilies = new HashMap();
        ColumnFamilySpec cf = new ColumnFamilySpec();
        cf.setName("links");
        columnFamilies.put("links", cf);

        cf = new ColumnFamilySpec();
        cf.setName("nickname");
        columnFamilies.put("nickname", cf);

        schema.setColumn_families(columnFamilies);

        try {
            client.table_create(ns, "datahubgs", schema);
        } catch (TException e) {
            e.printStackTrace();
        }

        Map<String, String> nickDict = new HashMap<String, String>();

        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(inputCSV));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        RequestConfig config = RequestConfig.custom().build();
        if (prop.containsKey("proxy_host") && prop.containsKey("proxy_port") && prop.containsKey("proxy_protocol")) {
            HttpHost proxy = new HttpHost(prop.getProperty("proxy_host"), Integer.parseInt(prop.getProperty("proxy_port")), prop.getProperty("proxy_protocol"));
            config = RequestConfig.custom().setProxy(proxy).build();
        }

        String line;

        try {
            while((line = br.readLine()) != null) {
                String[] sline = line.split(",");
                if (!sline[1].equals("") && !sline[4].equals("") && !sline[4].equals("*")) {
                    nickDict.put(sline[1].replace("http://datahub.io/dataset/", ""), sline[4]);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            br = new BufferedReader(new FileReader(inputCSV));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        try {
            List<Cell> cells = new ArrayList<>();
            while ((line = br.readLine()) != null) {
                String[] sline = line.split(",");
                if (!sline[1].equals("") && !sline[4].equals("") && !sline[4].equals("*")) {
		            HttpHost target = new HttpHost("datahub.io", 80, "http");
                    String url = String.format("%s%s", API_URL, sline[1].replace("http://datahub.io/dataset/", ""));
		            CloseableHttpClient httpclient = HttpClients.createDefault();
                    HttpGet request = new HttpGet(url);
                    request.setConfig(config);

		            CloseableHttpResponse response = httpclient.execute(target, request);

                    BufferedReader rd = new BufferedReader(
                            new InputStreamReader(response.getEntity().getContent()));

                    StringBuffer result = new StringBuffer();
                    String jsonLine = "";
                    while ((jsonLine = rd.readLine()) != null) {
                        result.append(jsonLine);
                    }

                    try {
                        String linkList = "";
                        JSONObject jsonObject = new JSONObject(result.toString());
                        JSONArray jsonResult = jsonObject.getJSONObject("result").getJSONArray("extras");
                        for (int i = 0; i < jsonResult.length(); i++) {
                            JSONObject extra = jsonResult.getJSONObject(i);
                            if (extra.getString("key").startsWith("links:")) {
                                String link = extra.getString("key").replace("links:", "");
                                if (nickDict.get(link) != null) {
                                    linkList += "," + nickDict.get(link);
                                }
                            }
                        }

                        System.out.println(String.format("%s - %s", sline[1].replace("http://datahub.io/dataset/", ""), linkList));

                        Key key = new Key();
                        key.setRow(sline[1].replace("http://datahub.io/dataset/", ""));
                        key.setColumn_family("links");
                        Cell cell = new Cell();
                        cell.setKey(key);

                        try {
                            cell.setValue(String.valueOf(linkList).getBytes("UTF-8"));
                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                        }

                        cells.add(cell);

                        if (sline.length >= 5) {
                            key = new Key();
                            key.setRow(sline[1].replace("http://datahub.io/dataset/", ""));
                            key.setColumn_family("nickname");
                            cell = new Cell();
                            cell.setKey(key);

                            try {
                                cell.setValue(sline[4].getBytes("UTF-8"));
                            } catch (UnsupportedEncodingException e) {
                                e.printStackTrace();
                            }

                            cells.add(cell);
                        }

                    } catch(JSONException e) {
                        e.printStackTrace();
                    }
                }
                //datasetList.add(sline[4]);
            }
            client.set_cells(ns, "datahubgs", cells);
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClientException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }
        try {
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
