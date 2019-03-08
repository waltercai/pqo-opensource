import java.io.Serializable;

public class Result implements Serializable{
    public String[] query;
    public int buckets;
    public double[] estimates;

    public Result(String[] _query, int _buckets, double[] _estimates){
        this.query = _query;
        this.buckets = _buckets;
        this.estimates = _estimates;
    }
}
