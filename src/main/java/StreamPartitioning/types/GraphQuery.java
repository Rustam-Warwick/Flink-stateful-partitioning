package StreamPartitioning.types;


/**
 * Represent a single user query to alter the graph.
 * @param element represents GraphElement that was modified. For now either Edge or Vertex
 * @param op represents the initial graph operation. For now Update, Delete, Add
 * @param res represent the result that should be returned after op is applied. For now NONE or NEIGHBORHOOD
 * @// TODO: 20.10.21 Can there be queries for vertices only or edges only? Maybe 2 separate sub classes. VertexQuery, EdgeQuery?
 */
public class GraphQuery {
    public GraphElement element; // Element over which we are querying
    public QueryOperation op = QueryOperation.NONE; // What we want to modify in partitions
    public QueryResult res = QueryResult.NONE; // What should be the result of our modification, that is streamed down the pipeline

    public GraphQuery(){
        this.element = null;
    }

    public GraphQuery(GraphElement element) {
        this.element = element;
    }


    public GraphQuery changeOperation(QueryOperation op){
        this.op = op;
        return this;
    }

    public GraphQuery changeResult(QueryResult res){
        this.res = res;
        return this;
    }
}
