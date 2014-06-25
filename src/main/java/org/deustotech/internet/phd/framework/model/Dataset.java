package org.deustotech.internet.phd.framework.model;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by mikel (m.emaldi at deusto dot es) on 25/06/14.
 */
public class Dataset {
    private int pk;
    private String name;
    private String url;

    public Dataset(String url, String name, int pk) {
        this.url = url;
        this.name = name;
        this.pk = pk;

    }


}
