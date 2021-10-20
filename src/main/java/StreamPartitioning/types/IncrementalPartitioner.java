package StreamPartitioning.types;


import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.state.PersistedValue;

/**
 * Generic Inteface for incremental partitioners, from the paper Incrementization of graph partitionin algorithms
 * Abstract class for incremental partitioning algorithms
 * @param <IS> Internal state that the algorithm stores
 * @param <INT> Intermediatry state after receiving the message
 *
 */



abstract public class IncrementalPartitioner<IS,INT> implements StatefulFunction {
    @Persisted
    private final PersistedValue<IS> state;

    public IncrementalPartitioner(Class <IS> persistedClassType) {
        this.state = PersistedValue.of("state",persistedClassType);
    }

    @Override
    public void invoke(Context context, Object o) {
        GraphQuery input = (GraphQuery) o;
        INT updateRegion = this.getUpdateRegion(input);
        String part = this.partition(updateRegion);
        context.send(Identifiers.PART_TYPE,part,input);
    }

    abstract public String partition(INT scope);
    abstract public INT getUpdateRegion(GraphQuery input);

}
