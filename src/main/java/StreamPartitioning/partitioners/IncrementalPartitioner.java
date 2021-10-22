package StreamPartitioning.partitioners;


import StreamPartitioning.types.UserQuery;
import StreamPartitioning.types.Identifiers;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.StatefulFunction;

/**
 * Generic Inteface for incremental partitioners, from the paper Incrementization of graph partitionin algorithms
 * Abstract class for incremental partitioning algorithms
 * @param <IS> Internal state that the algorithm stores
 * @param <INT> Intermediate state after receiving the message
 *
 *
 */
abstract public class IncrementalPartitioner<INT> implements StatefulFunction {
    public Short PART_SIZE = 10;

    @Override
    public void invoke(Context context, Object o) {
        UserQuery input = (UserQuery) o;
        System.out.println(input);
        INT updateRegion = this.getUpdateRegion(input);
        String part = this.partition(updateRegion);
        context.send(Identifiers.PART_TYPE,part,input);
    }

    abstract public String partition(INT scope);
    abstract public INT getUpdateRegion(UserQuery input);

}
