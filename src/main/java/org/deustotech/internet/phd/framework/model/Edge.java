package org.deustotech.internet.phd.framework.model;

/**
 * Created by mikel (m.emaldi at deusto dot es) on 20/06/14.
 */
public class Edge {
    private String label;
    private Vertex target;

    public Edge() {
    }

    public String getLabel() {
        return label;
    }

    public Edge(String label, Vertex target) {
        this.label = label;
        this.target = target;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public Vertex getTarget() {
        return target;
    }

    public void setTarget(Vertex target) {
        this.target = target;
    }
}
