package org.deustotech.internet.phd;

import org.deustotech.internet.phd.baselines.DistinctTriplesEqualityBaseline;
import org.deustotech.internet.phd.baselines.OntologyRankingBaseline;
import org.deustotech.internet.phd.baselines.PrefixComparisonBaseline;
import org.deustotech.internet.phd.framework.generatealignments.GenerateAlignments;
import org.deustotech.internet.phd.framework.loadsubgraphs.LoadSubgraphs;
import org.deustotech.internet.phd.framework.matchsubgraphs.MatchSubgraphs;
import org.deustotech.internet.phd.framework.rdf2subdue.RDF2Subdue;
import org.deustotech.internet.phd.validation.LoadDataHubGS;
import org.deustotech.internet.phd.validation.RelationExtractor;
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
                RDF2Subdue.run(args[1], args[2], Boolean.parseBoolean(args[3]), Boolean.parseBoolean(args[4]));
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
                GenerateAlignments.run(args[1], args[2], Boolean.parseBoolean(args[3]));
                break;
            case "testdistances":
                TestDistances.run(args[1]);
                break;
            case "matchsubgraphs":
                MatchSubgraphs.run(args[1], Boolean.parseBoolean(args[2]), Boolean.parseBoolean(args[3]), Boolean.parseBoolean(args[4]));
                break;
            case "loaddatahubgs":
                LoadDataHubGS.run(args[1]);
                break;
            case "relationextractor":
                RelationExtractor.run();
                break;
            case "prefixcomparisonbaseline":
                PrefixComparisonBaseline.run(Boolean.parseBoolean(args[1]));
                break;
            case "ontologyrankingbaseline":
                OntologyRankingBaseline.run(Boolean.parseBoolean(args[1]));
            case "distincttriplesequalitybaseline":
                DistinctTriplesEqualityBaseline.run();

        }
    }
}
