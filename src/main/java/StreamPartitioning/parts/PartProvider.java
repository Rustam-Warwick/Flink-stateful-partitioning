package StreamPartitioning.parts;

import StreamPartitioning.partitioners.RandomPartitioner;
import StreamPartitioning.storage.HashMapGraphStorage;
import org.apache.flink.statefun.flink.datastream.SerializableStatefulFunctionProvider;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;

public class PartProvider implements SerializableStatefulFunctionProvider {
    public PartProvider(){

    }

    @Override
    public StatefulFunction functionOfType(FunctionType functionType) {
        return new SimpleStoragePart().setStorage(new HashMapGraphStorage());
    }
}
