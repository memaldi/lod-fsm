package org.deustotech.internet.phd.baselines;

import org.deustotech.internet.phd.framework.rdf2subdue.RDF2Subdue;

/**
 * Created by mikel on 10/06/14.
 */
public class Main {

    private static  String CSV_LOCATION = "/home/mikel/doctorado/src/LDClassifier-python/LDClassifier-python/baselines/test-datasets.csv";
    private static String SPARQL_ENDPOINT = "http://helheim.deusto.es/sparql";

    public static void main(String[] args) {

        if (args.length > 0) {
            CSV_LOCATION = args[0];
        }
        //PrefixComparisonBaseline pcb = new PrefixComparisonBaseline();
        //pcb.launch(CSV_LOCATION);
        //DistinctTriplesEqualityBaseline dteb = new DistinctTriplesEqualityBaseline();
        //dteb.launch(CSV_LOCATION);
        RDF2Subdue rdf2Subdue = new RDF2Subdue();
        rdf2Subdue.launch("acm", "/home/mikel/doctorado/src/java/baselines/output");
    }
}
