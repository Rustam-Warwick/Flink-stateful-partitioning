package StreamPartitioning.vertex;

import StreamPartitioning.edges.Edge;
import StreamPartitioning.features.Feature;
import org.apache.flink.statefun.sdk.Context;

public interface BaseVertex{
    void addEdgeCallback(Edge e, Context c);
    void addVertexCallback(Context c);
    void updateFeatureCallback(Context c, Feature f);
}
