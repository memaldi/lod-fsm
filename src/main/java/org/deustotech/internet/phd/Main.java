package org.deustotech.internet.phd;

import org.deustotech.internet.phd.framework.generatealignments.GenerateAlignments;
import org.deustotech.internet.phd.framework.loadsubgraphs.LoadSubgraphs;
import org.deustotech.internet.phd.framework.matchsubgraphs.MatchSubgraphs;
import org.deustotech.internet.phd.framework.rdf2subdue.RDF2Subdue;
import org.deustotech.internet.phd.validation.LoadDataHubGS;
import org.deustotech.internet.phd.validation.TestDistances;

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

        switch (args[0].toLowerCase()) {
            case "rdf2subdue":
                if (args.length < 3) {
                    System.out.println("Invalid number of parameters!");
                    exit(1);
                }
                RDF2Subdue.run(args[1], args[2], Boolean.parseBoolean(args[3]));
                break;
            case "loadsubgraphs":
                if (args.length < 2) {
                    System.out.println("Invalid number of parameters!");
                    exit(1);
                }
                LoadSubgraphs.run(args[1]);
                break;
            case "generatealignments":
                if (args.length < 3) {
                    System.out.println("Invalid number of parameters!");
                    exit(1);
                }
                GenerateAlignments.run(args[1], args[2]);
                break;
            case "testdistances":
                TestDistances.run(args[1]);
                break;
            case "matchsubgraphs":
                MatchSubgraphs.run(Double.parseDouble(args[1]), args[2], Boolean.parseBoolean(args[3]), args[4], Integer.parseInt(args[5]));
                break;
            case "loaddatahubgs":
                LoadDataHubGS.run(args[1]);
                break;
        }
    }
}
