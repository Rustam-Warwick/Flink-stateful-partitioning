package StreamPartitioning.sources;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import java.util.Random;

/**
 * Dummy String Generator. Serves no purpose!
 */
public class StringGenerator extends RichParallelSourceFunction<String> {
    public Random random;
    public volatile boolean isRunning = true;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        random = new Random();
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while(isRunning){
            int a = random.nextInt();
            ctx.collect(String.valueOf(a));
        }
    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }
}

