package StreamPartitioning.aggregators.PartitionReportingAggregator;

import StreamPartitioning.aggregators.BaseAggregator;
import StreamPartitioning.parts.BasePart;
import StreamPartitioning.types.Vertex;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;

import java.util.Set;
import java.util.stream.Collectors;

public class PartitionReportingAggregator extends BaseAggregator {
    public static final EgressIdentifier<String> egress = new EgressIdentifier<>("partitioning", "reporting", String.class);


    @Override
    public boolean shouldTrigger(Object o) {
        return true;
    }

    @Override
    public void dispatch(Context ctx, Object msg) {
        return;
//        Set<String> remoteVertices = part.getStorage()
//                .getVertices().filter(item->(item.getPartId()!=null)&&(item.getPartId()!=-1))
//                .map(item->item.getId())
//                .collect(Collectors.toSet());
//
//        long countRemote = part
//                        .getStorage()
//                        .getEdges()
//                        .filter(item->remoteVertices.contains(item.destination._1))
//                        .count();
//
//        long countTotal = part.getStorage().getEdges().count();
//
//
//
//        ctx.send(this.egress,String.format("Part %s - Cut ratio: %s",ctx.self().id(),(double)countRemote/countTotal));
    }
}
