package org.deustotech.internet.phd.framework.loadsubgraphs;

import java.io.*;

/**
 * Created by mikel (m.emaldi at deusto dot es) on 17/06/14.
 */
public class LoadSubgraphs {
    public static void run(String inputDir) {
        File folder = new File(inputDir);
        for(File file : folder.listFiles()) {
            try {
                FileReader fileReader = new FileReader(file);
                BufferedReader br = new BufferedReader(fileReader);
                String line;
                while((line = br.readLine()) != null) {
                    
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
