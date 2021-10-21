package StreamPartitioning.parts;

import StreamPartitioning.storage.GraphStorage;
import org.apache.flink.statefun.sdk.StatefulFunction;

public abstract class IncrementalPart implements StatefulFunction {
    GraphStorage storage;

    public IncrementalPart setStorage(GraphStorage storage) {
        this.storage = storage;
        return this;
    }
}
