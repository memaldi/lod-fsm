package org.deustotech.internet.phd.validation;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.JSONArray;
import org.json.JSONObject;
import java.io.*;

/**
 * Created by mikel (m.emaldi at deusto dot es) on 26/08/14.
 */
public class LoadDataHubGT {

    private static String API_URL = "http://datahub.io/api/3/action/package_show?id=";

    public static void run(String inputCSV) {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(inputCSV));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        String line;
        try {
            while ((line = br.readLine()) != null) {
                String[] sline = line.split(",");
                if (!sline[1].equals("")) {
                    String url = String.format("%s%s", API_URL, sline[1].replace("http://datahub.io/dataset/", ""));
                    HttpClient client = new DefaultHttpClient();
                    HttpGet request = new HttpGet(url);
                    HttpResponse response = client.execute(request);

                    BufferedReader rd = new BufferedReader(
                            new InputStreamReader(response.getEntity().getContent()));

                    StringBuffer result = new StringBuffer();
                    String jsonLine = "";
                    while ((jsonLine = rd.readLine()) != null) {
                        result.append(jsonLine);
                    }

                    JSONObject jsonObject = new JSONObject(result.toString());
                    JSONArray jsonResult = jsonObject.getJSONObject("result").getJSONArray("extras");
                    for (int i = 0; i < jsonResult.length(); i++) {
                        JSONObject extra = jsonResult.getJSONObject(i);
                        if (extra.getString("key").startsWith("links:")) {
                            String link = extra.getString("key").replace("links:", "");
                            System.out.println(link);
                        }
                    }
                    System.out.println(result.toString());
                }
                //datasetList.add(sline[4]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
