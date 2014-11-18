package org.deustotech.internet.phd.validation;

import org.deustotech.internet.phd.framework.matchsubgraphs.MatchSubgraphs;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by mikel (m.emaldi at deusto dot es) on 27/10/14.
 */
public class RelationExtractor {
    public static void run() {
        Map<String, Map<String, Float>> resultMap = new HashMap<>();
        Map<String, List<String>> goldStandard = MatchSubgraphs.loadGoldStandard();
        int count = 0;
        File file = new File("relations.csv");
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter(file));
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (String sourceDataset : goldStandard.keySet()) {
            List<String> sourceDatasetList = goldStandard.get(sourceDataset);
            for (String targetDataset : sourceDatasetList) {
                List<String> targetDatasetList = new ArrayList<>(goldStandard.get(targetDataset));
                int targetDatasetListSize = targetDatasetList.size();
                targetDatasetList.retainAll(sourceDatasetList);
                float score = (float) targetDatasetList.size() / sourceDatasetList.size();
                if (score > 0 && score < 1) {
                    //System.out.println(String.format("%s - %s (%s)", sourceDataset, targetDataset, score));

                    if (!resultMap.containsKey(sourceDataset)) {
                        resultMap.put(sourceDataset, new HashMap<String, Float>());
                    }
                    if (resultMap.containsKey(targetDataset)) {
                        if(!resultMap.get(targetDataset).containsKey(sourceDataset)) {
                            Map<String, Float> scoreMap = resultMap.get(sourceDataset);
                            scoreMap.put(targetDataset, score);
                            resultMap.put(sourceDataset, scoreMap);
                            count++;
                        }
                    } else {
                        Map<String, Float> scoreMap = resultMap.get(sourceDataset);
                        scoreMap.put(targetDataset, score);
                        resultMap.put(sourceDataset, scoreMap);
                        count++;
                    }

                }
            }
        }

        for (String source : resultMap.keySet()) {
            for (String target : resultMap.get(source).keySet()) {
                try {
                    bw.write(String.format("%s;%s;%s\n", source, target, resultMap.get(source).get(target)));
                } catch (IOException e) {
                    e.printStackTrace();
                }
                System.out.println(String.format("%s - %s (%s)", source, target, resultMap.get(source).get(target)));
            }
        }
        try {
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(String.format("Total: %s", count));
    }
}
