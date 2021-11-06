package StreamPartitioning.storage;

import StreamPartitioning.edges.Edge;
import StreamPartitioning.features.Feature;
import StreamPartitioning.vertex.BaseReplicatedVertex;
import org.apache.flink.statefun.sdk.Context;

import java.util.stream.Stream;

public interface GraphStorage {
    // CRUD Related Stuff
    boolean addVertex(BaseReplicatedVertex v, Context c);
    void deleteVertex(BaseReplicatedVertex v,Context c);
    void updateVertex(Feature f,Context c);
    void addEdge(Edge e,Context c);
    void deleteEdge(Edge e,Context c);
    void updateEdge(Edge e,Context c);

    // Query Related Stuff
    BaseReplicatedVertex getVertex(String id);
    Stream<BaseReplicatedVertex> getVertices();
    Stream<Edge> getEdges();
    Edge getEdge();


}
