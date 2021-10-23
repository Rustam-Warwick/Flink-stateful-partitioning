package StreamPartitioning.partitioners;

import StreamPartitioning.types.UserQuery;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.match.StatefulMatchFunction;

abstract public class BasePartitioner extends StatefulMatchFunction {
    public short NUM_PARTS =  10;

    public BasePartitioner setNUM_PARTS(short NUM_PARTS) {
        this.NUM_PARTS = NUM_PARTS;
        return this;
    }

}
