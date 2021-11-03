package StreamPartitioning.vertex;
import StreamPartitioning.types.Identifiers;
import org.apache.flink.statefun.sdk.Context;
import StreamPartitioning.types.GraphElement;

import java.util.ArrayList;
import java.util.Date;
import java.util.stream.Stream;

/**
 * Vertex of graph that is also a GraphElement
 */
public class Vertex extends BaseReplicatedVertex {



    @Override
    public Object getFeature(Short l) {
        return null;
    }

    @Override
    public void mergeWithIncoming(BaseReplicatedVertex incoming){
        // 1. Add all incoming new part table updates to this
        incoming.inParts.forEach(item->{
            if(!this.inParts.contains(item))this.inParts.add(item);
        });
        incoming.outParts.forEach(item->{
            if(!this.outParts.contains(item))this.outParts.add(item);
        });
        if(this.lastSync==null){
            this.lastSync = incoming.lastSync;
            return;
        }
        if(this.lastSync!=null && incoming.lastSync==null)return;



    }

}