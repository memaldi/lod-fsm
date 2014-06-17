package org.deustotech.internet.phd;

import org.deustotech.internet.phd.framework.rdf2subdue.RDF2Subdue;

import static java.lang.System.exit;

/**
 * Created by mikel on 10/06/14.
 */
public class Main {

    private static  String CSV_LOCATION = "/home/mikel/doctorado/src/LDClassifier-python/LDClassifier-python/baselines/test-datasets.csv";
    private static String SPARQL_ENDPOINT = "http://helheim.deusto.es/sparql";

    public static void main(String[] args) {

        if (args.length <= 0) {
            System.out.println("Invalid number of parameters!");
            exit(1);
        }

        switch (args[0]) {
            case "rdf2subdue":
                if (args.length < 3) {
                    System.out.println("Invalid number of parameters!");
                    exit(1);
                }
                RDF2Subdue.run(args[1], args[2]);
                break;
        }
    }
}
