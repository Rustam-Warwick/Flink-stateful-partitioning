package StreamPartitioning.storage;

import StreamPartitioning.types.Edge;
import StreamPartitioning.types.Vertex;

import java.util.function.Predicate;
import java.util.stream.Stream;

public interface GraphStorage {
    // CRUD Related Stuff
    void addVertex(Vertex v);
    void deleteVertex(Vertex v);
    void updateVertex(Vertex v);
    void addEdge(Edge e);
    void deleteEdge(Edge e);
    void updateEdge(Edge e);

    // Query Related Stuff
    Stream<Vertex> getVertices();
    Stream<Edge> getEdges();

}
