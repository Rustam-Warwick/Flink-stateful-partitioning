package StreamPartitioning.storage;

import StreamPartitioning.types.Edge;
import StreamPartitioning.types.Vertex;

public interface GraphStorage {
//    void setPartId(Integer id);
    void addVertex(Vertex v);
    void deleteVertex(Vertex v);
    void updateVertex(Vertex v);
    void addEdge(Edge e);
    void deleteEdge(Edge e);
    void updateEdge(Edge e);
}
