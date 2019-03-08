import java.util.ArrayList;

public class Relation {
    String alias;
    String tableName;
    ArrayList<Attribute> joinAttributes;
    ArrayList<Attribute> keys;
    ArrayList<Filter> filters;

    Relation(String alias){
        this.alias = alias;
        this.joinAttributes = new ArrayList<>();
        this.keys = new ArrayList<>();
        this.filters = new ArrayList<>();
    }

    @Override
    public String toString(){
        return alias.toString();
    }

    @Override
    public boolean equals(Object r){
        if(r instanceof Relation){
            if(((Relation) r).alias.equals(this.alias)){
                return true;
            }
        }
        return false;
    }

    void print(){
        System.out.print(String.format("%s AS %s ", this.tableName, this.alias));
        System.out.print("| joinAttributes: ");
        for(Attribute a: this.joinAttributes) {
            System.out.print(a.index + ", ");
        }
        if(this.keys.size() > 0){
            System.out.print("| keys: ");
            for(Attribute a: this.keys) {
                System.out.print(a.index + ", ");
            }
        }
        System.out.print("| filters: ");
        for(Filter f: this.filters){
            System.out.print(f.getPredicate() + ", ");
        }
        System.out.println();
    }
}
