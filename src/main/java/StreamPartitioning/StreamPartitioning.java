package StreamPartitioning;

import StreamPartitioning.partitioners.RandomPartitioner;
import StreamPartitioning.parts.SimpleStoragePart;
import StreamPartitioning.sources.GraphGenerator;
import StreamPartitioning.storage.HashMapGraphStorage;
import StreamPartitioning.types.UserQuery;
import StreamPartitioning.types.Identifiers;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.statefun.flink.core.StatefulFunctionsConfig;
import org.apache.flink.statefun.flink.core.message.MessageFactoryType;
import org.apache.flink.statefun.flink.core.message.RoutableMessage;
import org.apache.flink.statefun.flink.core.message.RoutableMessageBuilder;
import org.apache.flink.statefun.flink.datastream.StatefulFunctionDataStreamBuilder;
import org.apache.flink.statefun.flink.datastream.StatefulFunctionEgressStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.OutputStream;

/**
 * @// TODO: 21/10/2021 Introduce new Types for input. UserQuery, GraphQuery and etc.
 */
public class StreamPartitioning {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        DataStream<UserQuery> stream = env.addSource(new GraphGenerator()).returns(Types.POJO(UserQuery.class));

        DataStream<RoutableMessage> ingress = stream.map(graphElement->
           RoutableMessageBuilder
                   .builder()
                   .withTargetAddress(Identifiers.PARTITIONER_TYPE,"1")
                   .withMessageBody(graphElement)
                   .build()
        );

        StatefulFunctionsConfig config = StatefulFunctionsConfig.fromEnvironment(env);

        config.setFactoryType(MessageFactoryType.WITH_KRYO_PAYLOADS);

        StatefulFunctionEgressStreams res = StatefulFunctionDataStreamBuilder
                .builder("partitioning")
                .withDataStreamAsIngress(ingress)
                .withFunctionProvider(Identifiers.PART_TYPE,(param)->new RandomPartitioner().setNUM_PARTS((short)8))
                .withFunctionProvider(Identifiers.PARTITIONER_TYPE,(param)->new SimpleStoragePart().setStorage(new HashMapGraphStorage()))
                .withConfiguration(config)
                .build(env);


        System.out.println(env.getExecutionPlan());
        env.execute();




    }

}
