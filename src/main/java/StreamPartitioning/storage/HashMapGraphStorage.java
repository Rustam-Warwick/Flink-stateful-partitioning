package StreamPartitioning.storage;

import StreamPartitioning.types.Edge;
import StreamPartitioning.types.Vertex;

import java.util.ArrayList;
import java.util.HashMap;

public class HashMapGraphStorage implements GraphStorage {
    HashMap<Vertex, ArrayList<Edge>> graph;
    public HashMapGraphStorage() {
        graph = new HashMap<Vertex,ArrayList<Edge>>();
    }

    @Override
    public void addVertex(Vertex v) {

    }

    @Override
    public void deleteVertex(Vertex v) {

    }

    @Override
    public void updateVertex(Vertex v) {

    }

    @Override
    public void addEdge(Edge e) {
        if(graph.containsKey(e.source)){
            graph.get(e.source).add(e);
        }else{
            graph.put(e.source,new ArrayList<Edge>(){{add(e);}});
        }
        if(!graph.containsKey(e.destination)){
            graph.put(e.destination,new ArrayList<Edge>());
        }
    }

    @Override
    public void deleteEdge(Edge e) {

    }

    @Override
    public void updateEdge(Edge e) {

    }
}
