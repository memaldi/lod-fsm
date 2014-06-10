package org.deustotech.internet.phd.baselines;

/**
 * Created by mikel on 10/06/14.
 */
public class Main {

    private static  String CSV_LOCATION = "/home/mikel/doctorado/src/LDClassifier-python/LDClassifier-python/baselines/test-datasets.csv";
    private static String SPARQL_ENDPOINT = "http://helheim.deusto.es/sparql";

    public static void main(String[] args) {

        PrefixComparisonBaseline pcb = new PrefixComparisonBaseline();
        pcb.launch(CSV_LOCATION);
    }
}
