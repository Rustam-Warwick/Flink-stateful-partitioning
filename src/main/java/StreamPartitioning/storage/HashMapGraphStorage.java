package StreamPartitioning.storage;

import StreamPartitioning.types.Edge;
import StreamPartitioning.vertex.BaseReplicatedVertex;
import StreamPartitioning.vertex.Vertex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.Stream;

/**
 * HashMap Based Vertex-Centric graph storage
 */
public class HashMapGraphStorage<VT extends BaseReplicatedVertex> implements GraphStorage<VT> {
    /**
     * Stores Edges as a map of (source_key=>(dest_key))
     */
    public HashMap<String, ArrayList<String>> edges;
    /**
     * Stores Vertex hashed by source id. Good for O(1) search
     * Note that dest vertices are stored here as well with the isPart attribute set to something else
     */
    public HashMap<String, VT> vertices;

    public HashMapGraphStorage() {
        edges = new HashMap<>();
        vertices = new HashMap<>();
    }

    @Override
    public void addVertex(VT v) {
        // If vertex is already here then discard it
        if(vertices.containsKey(v.getId()))return;
        vertices.put(v.getId(),v);
    }

    @Override
    public void deleteVertex(VT v) {

    }

    @Override
    public void updateVertex(VT v) {
        // 1. If vertex DNE simply discard it???
        if(!vertices.containsKey(v.getId()))return;
        // 3. Replace the old vertex with this one
        vertices.put(v.getId(),v);
    }

    @Override
    public void addEdge(Edge<VT> e) {
        // 1. If source vertex not in storage create it
        if(!vertices.containsKey(e.source.getId())){
            vertices.put(e.source.getId(),e.source);
        }
        // 2. Do same for destination
        if(!vertices.containsKey(e.destination.getId())){
            vertices.put(e.destination.getId(),e.destination);
        }
        // 3. Put the edge to the edge list
        edges.putIfAbsent(e.source.getId(),new ArrayList<>());
        edges.get(e.source.getId()).add(e.destination.getId());

    }

    @Override
    public void deleteEdge(Edge<VT> e) {

    }

    @Override
    public void updateEdge(Edge<VT> e) {
        // Meaningless until we have edge features

    }

    @Override
    public VT getVertex(String id) {
        return this.vertices.get(id);
    }

    @Override
    public Edge<VT> getEdge() {
        return null;
    }

    @Override
    public Stream<VT> getVertices() {
        return vertices.values().stream();
    }

    @Override
    public Stream<Edge<VT>> getEdges() {
        return null;
//        return edges.entrySet().stream().flatMap((item)->(
//            item
//            .getValue()
//            .stream()
//            .map(a->new Edge().betweenVertices(new Vertex().withId(item.getKey()),new Vertex().withId(a))))
//            );
    }
}
