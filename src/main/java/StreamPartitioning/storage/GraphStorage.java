package StreamPartitioning.storage;

import StreamPartitioning.types.Edge;
import StreamPartitioning.vertex.BaseReplicatedVertex;
import StreamPartitioning.vertex.Vertex;

import java.util.stream.Stream;

public interface GraphStorage<VT extends BaseReplicatedVertex> {
    // CRUD Related Stuff
    void addVertex(VT v);
    void deleteVertex(VT v);
    void updateVertex(VT v);
    void addEdge(Edge<VT> e);
    void deleteEdge(Edge<VT> e);
    void updateEdge(Edge<VT> e);

    // Query Related Stuff
    VT getVertex(String id);
    Stream<VT> getVertices();
    Stream<Edge<VT>> getEdges();
    Edge<VT> getEdge();


}
