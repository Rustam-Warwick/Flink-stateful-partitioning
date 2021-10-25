package StreamPartitioning.storage;

import StreamPartitioning.types.Edge;
import StreamPartitioning.types.Vertex;
import scala.Int;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * HashMap Based Vertex-Centric graph storage
 */
public class HashMapGraphStorage implements GraphStorage {
    /**
     * Stores Edges as a map of (source_key=>(dest_key))
     */
    HashMap<String, ArrayList<String>> edges;
    /**
     * Stores Vertex hashed by source id. Good for O(1) search
     * Note that dest vertices are stored here as well with the isPart attribute set to something else
     */
    HashMap<String, Vertex> vertices;

    public HashMapGraphStorage() {
        edges = new HashMap<>();
        vertices = new HashMap<>();
    }


    @Override
    public void addVertex(Vertex v) {
        if(v.getPartId()==null)v.inPart(-1); // Indicating that vertex is not remote and actually came to this partition
        vertices.put(v.getId(),v);
    }

    @Override
    public void deleteVertex(Vertex v) {

    }

    @Override
    public void updateVertex(Vertex v) {

    }

    @Override
    public void addEdge(Edge e) {
        // 1. If source vertex not in storage create a placeholder object
        if(!vertices.containsKey(e.source._1)){
            vertices.put(e.source._1,new Vertex().withId(e.source._1)); // Don't touch the part since it will be changed once vertex arrives
        }
        // 2. Same for destination
        if(!vertices.containsKey(e.destination._1)){
            vertices.put(e.destination._1,new Vertex().withId(e.destination._1).inPart(e.destination._2));
        }
        // 3. Put the edge to the edge list
        edges.putIfAbsent(e.source._1,new ArrayList<>());
        edges.get(e.source._1).add(e.destination._1);

    }

    @Override
    public void deleteEdge(Edge e) {

    }

    @Override
    public void updateEdge(Edge e) {

    }
}
