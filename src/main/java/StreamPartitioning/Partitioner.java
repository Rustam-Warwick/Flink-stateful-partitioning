package StreamPartitioning;

import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

import StreamPartitioning.types.Edge;
import StreamPartitioning.types.Vertex;
import org.apache.flink.statefun.sdk.java.Context;

import org.apache.flink.statefun.sdk.java.StatefulFunction;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.ValueSpec;
import org.apache.flink.statefun.sdk.java.message.Message;

public class Partitioner implements StatefulFunction {

    static final TypeName TYPE = TypeName.typeNameFromString("partitioning/partitioner"); // Type of the current Function namespace/name
    static final Short K = 12;
    private final HashMap<Integer,Short> PARTITION_TABLE  = new HashMap<Integer,Short>();
    static final ValueSpec<Integer> SEEN = ValueSpec.named("seen").withIntType(); // This is scoped to the address and is Integer type

    public static Object finalparseInputData(String msg) throws Exception{
        String [] parts = msg.split(",");
        switch (parts.length){
            case 1:
//                This is a new vertex
                    return new Vertex(Integer.parseInt(parts[0]));
            case 3:
//                This is a new edge for existing vertices
                return new Edge(Integer.parseInt(parts[0]),Integer.parseInt(parts[1]),Float.parseFloat(parts[2]));
            default:
                throw new Exception();
        }
    }

    public static Integer assignVertex(Vertex m){

        return 1;
    }


    @Override
    public CompletableFuture<Void> apply(Context context, Message message) {
        try{
            String stringMessage = message.asUtf8String(); // Expecting either a new vertex or a new edge
            Object obj = Partitioner.finalparseInputData(stringMessage);
            switch (obj.getClass().toString()){
                case "StreamPartitioning.types.Vertex":

                case "StreamPartitioning.types.Edge":
                default:
                    throw new Exception();
            }
        }
        catch (Exception e){

        }
        return context.done();
    }
}