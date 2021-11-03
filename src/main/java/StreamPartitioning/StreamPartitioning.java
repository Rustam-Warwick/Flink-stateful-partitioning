package StreamPartitioning;

import StreamPartitioning.aggregators.GNNAggregator.BaseGNNAggregator;
import StreamPartitioning.aggregators.PartitionReportingAggregator.PartitionReportingAggregator;
import StreamPartitioning.partitioners.RandomVertexCutPartitioner;
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

/**
 * @// TODO: #done 29/10/2021 Vertex-cut partitioning
 * @// TODO: #done 29/10/2021 Implements push based streaming GNN-algorithm
 * @// TODO: 29/10/2021 Implements serveral HDRF like algorithms
 * @// TODO: 29/10/2021 Test and think about data consistnecy
 */
public class StreamPartitioning {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(10);
        env.getConfig().disableClosureCleaner();
        StatefulFunctionsConfig config = StatefulFunctionsConfig.fromEnvironment(env);
        config.setFactoryType(MessageFactoryType.WITH_KRYO_PAYLOADS);


        DataStream<UserQuery> stream = env.addSource(new GraphGenerator()).returns(Types.POJO(UserQuery.class));

        DataStream<RoutableMessage> ingress = stream.map(graphElement->
           RoutableMessageBuilder
                   .builder()
                   .withTargetAddress(Identifiers.PARTITIONER_TYPE,"1")
                   .withMessageBody(graphElement)
                   .build()
        );


        StatefulFunctionEgressStreams res = StatefulFunctionDataStreamBuilder
                .builder("partitioning")
                .withDataStreamAsIngress(ingress)
                .withFunctionProvider(Identifiers.PARTITIONER_TYPE,(param)->new RandomVertexCutPartitioner().setNUM_PARTS((short)8))
                .withFunctionProvider(Identifiers.PART_TYPE,(param)->
                        new SimpleStoragePart()
                                .setStorage(new HashMapGraphStorage())
                                .attachAggregator(new BaseGNNAggregator())
                                )
                .withConfiguration(config)
                .withEgressId(PartitionReportingAggregator.egress)
                .build(env);

        DataStream<String> greetingsEgress = res.getDataStreamForEgressId(PartitionReportingAggregator.egress);
        greetingsEgress.print();



        env.execute();




    }

}
