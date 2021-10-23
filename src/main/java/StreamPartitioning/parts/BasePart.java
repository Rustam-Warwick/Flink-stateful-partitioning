package StreamPartitioning.parts;

import StreamPartitioning.storage.GraphStorage;
import StreamPartitioning.types.UserQuery;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.match.StatefulMatchFunction;


/**
 * Base class for Part Types
 */
abstract public class BasePart extends StatefulMatchFunction {
    GraphStorage storage = null;

    public BasePart setStorage(GraphStorage storage) {
        this.storage = storage;
        return this;
    }

    public GraphStorage getStorage() {
        if(storage==null) throw new NotImplementedException("Add a storage to graph part");
        return storage;
    }

    abstract public void dispatch(Context c, UserQuery query);





}
