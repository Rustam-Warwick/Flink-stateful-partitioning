package StreamPartitioning.types;

import StreamPartitioning.features.Feature;

public class VertexFeatureSync<T> {
    public String vertexId;
    public Feature feature;


    public VertexFeatureSync(String vertexId, Feature feature) {
        this.vertexId = vertexId;
        this.feature = feature;
    }
}
