package StreamPartitioning.modules;

import StreamPartitioning.partitioners.PartitionerProvider;
import StreamPartitioning.parts.PartProvider;
import StreamPartitioning.types.Identifiers;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;

import java.util.Map;

public class PartitioningModule implements StatefulFunctionModule {

    @Override
    public void configure(Map<String, String> globalConfiguration, Binder binder) {
        binder.bindFunctionProvider(Identifiers.PARTITIONER_TYPE,new PartitionerProvider());
        binder.bindFunctionProvider(Identifiers.PART_TYPE,new PartProvider());
    }
}
