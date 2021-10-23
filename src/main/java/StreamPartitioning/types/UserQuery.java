package StreamPartitioning.types;


/**
 * Represent a single user query to alter the graph.
 * @param element represents GraphElement that was modified. For now either Edge or Vertex
 * @param op represents the initial graph operation. For now Update, Delete, Add
 * @param res represent the result that should be returned after op is applied. For now NONE or NEIGHBORHOOD
 * @// TODO: 20.10.21 Can there be queries for vertices only or edges only? Maybe 2 separate sub classes. VertexQuery, EdgeQuery?
 */
public class UserQuery {
    public enum OPERATORS {NONE, ADD, REMOVE,UPDATE}

    public GraphElement element; // Element over which we are querying
    public OPERATORS op = OPERATORS.NONE;
    public UserQuery(){
        this.element = null;
    }

    public UserQuery(GraphElement element) {
        this.element = element;
    }

    public UserQuery changeOperation(OPERATORS op){
        this.op = op;
        return this;
    }
}
