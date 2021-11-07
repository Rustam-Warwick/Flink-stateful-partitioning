package StreamPartitioning.storage;

import StreamPartitioning.edges.Edge;
import StreamPartitioning.features.Feature;
import StreamPartitioning.vertex.BaseReplicatedVertex;
import org.apache.flink.statefun.sdk.Context;

import java.util.stream.Stream;

public interface GraphStorage<VT extends BaseReplicatedVertex> {
    // CRUD Related Stuff
    boolean addVertex(VT v, Context c);
    void deleteVertex(VT v,Context c);
    void updateVertex(Feature f,Context c);
    void addEdge(Edge<VT> e,Context c);
    void deleteEdge(Edge<VT> e,Context c);
    void updateEdge(Edge<VT> e,Context c);

    // Query Related Stuff
    VT getVertex(String id);
    Stream<VT> getVertices();
    Stream<Edge<VT>> getEdges();
    Edge<VT> getEdge();


}
