package StreamPartitioning.vertex;

import StreamPartitioning.types.Edge;
import StreamPartitioning.types.GraphElement;
import StreamPartitioning.features.Feature;
import org.apache.flink.statefun.sdk.Context;

abstract public class BaseVertex extends GraphElement {
    public BaseVertex(String id,Short partId){
        super(id,partId);
    }
    public BaseVertex(String id) {
        super(id);
    }

    abstract public void addEdgeCallback(Edge e, Context c);
    abstract  public void addVertexCallback(Context c);
    abstract public void updateVertexCallback(Context c, Feature f);
}
