package StreamPartitioning.storage;

import StreamPartitioning.types.Edge;
import StreamPartitioning.features.Feature;
import StreamPartitioning.vertex.BaseReplicatedVertex;
import org.apache.flink.statefun.sdk.Context;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.Stream;

/**
 * HashMap Based Vertex-Centric graph storage
 */
public class HashMapGraphStorage implements GraphStorage {
    /**
     * Stores Edges as a map of (source_key=>(dest_key))
     */
    public HashMap<String, ArrayList<String>> edges;
    /**
     * Stores Vertex hashed by source id. Good for O(1) search
     * Note that dest vertices are stored here as well with the isPart attribute set to something else
     */
    public HashMap<String, BaseReplicatedVertex> vertices;

    public HashMapGraphStorage() {
        edges = new HashMap<>();
        vertices = new HashMap<>();
    }

    @Override
    public boolean addVertex(BaseReplicatedVertex v, Context c) {
        // If vertex is already here then discard it
        if(vertices.containsKey(v.getId()))return false;
        BaseReplicatedVertex vC = v.copy();
        vertices.put(v.getId(), vC);
        vC.addVertexCallback(c);
        return true;
    }

    @Override
    public void deleteVertex(BaseReplicatedVertex v,Context c) {

    }

    @Override
    public void updateVertex(Feature f, Context c) {
        if(!vertices.containsKey(f.attachedId))return;
        getVertex(f.attachedId).updateVertexCallback(c,f);
    }

    @Override
    public void addEdge(Edge e,Context c) {
        // 1. If source vertex not in storage create it
        this.addVertex(e.source,c);
        this.addVertex(e.destination,c);
        edges.putIfAbsent(e.source.getId(),new ArrayList<>());
        edges.get(e.source.getId()).add(e.destination.getId());
        this.getVertex(e.source.getId()).addEdgeCallback(e,c);
        this.getVertex(e.destination.getId()).addEdgeCallback(e,c);
    }

    @Override
    public void deleteEdge(Edge e,Context c) {

    }

    @Override
    public void updateEdge(Edge e,Context c) {
        // Meaningless until we have edge features

    }

    // Get Queries
    @Override
    public BaseReplicatedVertex getVertex(String id) {
        return this.vertices.get(id);
    }

    @Override
    public Edge getEdge() {
        return null;
    }


    @Override
    public Stream<BaseReplicatedVertex> getVertices() {
        return vertices.values().stream();
    }

    @Override
    public Stream<Edge> getEdges() {
        return edges.entrySet().stream().flatMap(item->(
           item.getValue().stream()
                   .map(a->new Edge(getVertex(item.getKey()),getVertex(a)))
        ));
    }
}
