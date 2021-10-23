package StreamPartitioning.partitioners;

import StreamPartitioning.types.Identifiers;
import StreamPartitioning.types.UserQuery;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.match.MatchBinder;

import java.util.Random;


public class RandomPartitioner extends BasePartitioner{
    public Random random;

    public RandomPartitioner(){
        random = new Random();
    }

    @Override
    public void configure(MatchBinder matchBinder) {
        matchBinder.predicate(UserQuery.class,this::partition);
    }


    public void partition(Context c, UserQuery query){
        String partId = String.valueOf(random.nextInt(this.NUM_PARTS));
        c.send(Identifiers.PART_TYPE,partId,query);
    }
}
