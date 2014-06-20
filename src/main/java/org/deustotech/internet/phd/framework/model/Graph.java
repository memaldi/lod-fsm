package org.deustotech.internet.phd.framework.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by mikel (m.emaldi at deusto dot es) on 20/06/14.
 */
public class Graph {
    private String name;
    private List<Vertex> vertices;

    public Graph() {
        this.vertices = new ArrayList<>();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Vertex> getVertices() {
        return vertices;
    }

    public void setVertices(List<Vertex> vertices) {
        this.vertices = vertices;
    }

    public Vertex getVertex(int vertexId) {
        for (Vertex vertex : this.vertices) {
            if (vertex.getId() == vertexId) {
                return vertex;
            }
        }
        return null;
    }

    public List<Edge> getEdges() {
        List<Edge> edgeList = new ArrayList<>();
        for (Vertex vertex : this.vertices) {
            edgeList.addAll(vertex.getEdges());
        }
        return edgeList;
    }
}
