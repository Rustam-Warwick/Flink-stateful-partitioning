package StreamPartitioning.types;

public class Vertex {
    Integer id;
    public Vertex(Integer id){
        this.id = id;
    }

    public static void main(String[] args) {
        Object a = new Vertex(2);
        System.out.println(a.getClass());
    }
}