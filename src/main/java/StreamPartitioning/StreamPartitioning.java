package StreamPartitioning;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamPartitioning {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
//        env.getConfig().disableClosureCleaner();
//        DataStream<String> queries = env.addSource(new StringGenerator());
        DataStream<String> queries = env.socketTextStream("localhost:8000",8000);
        queries.map(
                new MapFunction<String, Object>() {
                    @Override
                    public Object map(String value) throws Exception {
                        System.out.println(value);
                        return value;
                    }
                }

        ).print();
        env.execute();




    }

}
