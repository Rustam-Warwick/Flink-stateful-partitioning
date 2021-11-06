package StreamPartitioning.vertex;

import StreamPartitioning.edges.Edge;
import StreamPartitioning.features.Feature;
import org.apache.flink.statefun.sdk.Context;

public interface BaseVertex{
    abstract public void addEdgeCallback(Edge e, Context c);
    abstract  public void addVertexCallback(Context c);
    abstract public void updateFeatureCallback(Context c, Feature f);
}
