package StreamPartitioning.features;

import StreamPartitioning.types.GraphElement;
import org.apache.flink.statefun.sdk.Context;

import java.util.concurrent.CompletableFuture;

abstract public class Feature<T> {
    abstract public CompletableFuture<T> getValue();
    abstract public void setValue(Feature<T> v, Context context);

    public String fieldName;
    public String attachedId=null;
    public Class<? extends GraphElement> attachedToClass=null;

    public Feature(String fieldName) {
        this.fieldName = fieldName;
    }
}
