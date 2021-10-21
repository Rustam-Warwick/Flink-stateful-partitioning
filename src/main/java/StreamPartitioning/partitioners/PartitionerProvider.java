package StreamPartitioning.partitioners;

import StreamPartitioning.partitioners.RandomPartitioner;
import org.apache.flink.statefun.flink.datastream.SerializableStatefulFunctionProvider;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;

public class PartitionerProvider implements SerializableStatefulFunctionProvider {
    public Short PART_SIZE = 10;
    public PartitionerProvider(){

    }
    public PartitionerProvider(Short PART_SIZE){
        PART_SIZE = PART_SIZE;
    }

    @Override
    public StatefulFunction functionOfType(FunctionType functionType) {
        return new RandomPartitioner(this.PART_SIZE);
    }
}
