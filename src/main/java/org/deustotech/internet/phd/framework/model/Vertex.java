package org.deustotech.internet.phd.framework.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by mikel (m.emaldi at deusto dot es) on 20/06/14.
 */
public class Vertex {
    private String label;
    private int id;
    private List<Edge> edges;

    public Vertex() {
        this.edges = new ArrayList<>();
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public List<Edge> getEdges() {
        return edges;
    }

    public void setEdges(List<Edge> edges) {
        this.edges = edges;
    }
}
