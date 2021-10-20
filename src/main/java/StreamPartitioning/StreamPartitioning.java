package StreamPartitioning;

import StreamPartitioning.partitioners.TestPartitioner;
import StreamPartitioning.sources.GraphGenerator;
import StreamPartitioning.types.GraphQuery;
import StreamPartitioning.types.Identifiers;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.statefun.flink.core.message.RoutableMessage;
import org.apache.flink.statefun.flink.core.message.RoutableMessageBuilder;
import org.apache.flink.statefun.flink.datastream.SerializableStatefulFunctionProvider;
import org.apache.flink.statefun.flink.datastream.StatefulFunctionDataStreamBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
public class StreamPartitioning {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
//        env.getConfig().disableClosureCleaner();
       DataStream<GraphQuery> stream = env.addSource(new GraphGenerator());
       DataStream<RoutableMessage> ingress = stream.map(graphElement->
                       RoutableMessageBuilder
                               .builder()
                               .withTargetAddress(Identifiers.PARTITIONER_TYPE,"1")
                               .withMessageBody(graphElement)
                               .build()
               );

        StatefulFunctionDataStreamBuilder
                .builder("partitioning")
                .withDataStreamAsIngress(ingress)
                .withFunctionProvider(Identifiers.PARTITIONER_TYPE,new TestPartitioner())
                .build(env);

        env.execute();




    }

}
