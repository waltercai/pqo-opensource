import java.io.Serializable;
import java.util.HashMap;

public class Sketch implements Serializable{
    String dbName;
    String activeCol;
    public String name;
    String[] attrs;
    String path;
    int[] hashSizes;
    HashMap< Integer, HashMap<Integer, Integer>> l2gIndex;
    HashMap< Integer, HashMap<Integer, Integer>> g2lIndex;

    public Sketch(String _name,
                  String _activeAttrs,
                  String[] _attrs,
                  String _dbName,
                  int[] _hashSizes){
        this.name = _name;
        this.activeCol = _activeAttrs;
        this.attrs = _attrs.clone();
        this.dbName = _dbName;
        this.hashSizes = _hashSizes;
        this.l2gIndex = new HashMap<>();
        this.g2lIndex = new HashMap<>();
    }

    public Sketch(String _name,
                  String _dbName,
                  int[] _hashSizes){
        this.name = _name;
        this.dbName = _dbName;
        this.hashSizes = _hashSizes;
        this.l2gIndex = new HashMap<>();
    }

    public void quickDescribe(){
    }

    public long access(int boundID, int gVarIndex, int[] arr){
        return 0;
    }

    public void serialize(Relation r){}
}
