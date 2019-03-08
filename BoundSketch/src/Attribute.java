import java.util.ArrayList;

public class Attribute {
    int index;
    ArrayList<Relation> covers;
    ArrayList<String> cols;

    Attribute(int index){
        this.index = index;
        this.covers = new ArrayList<>();
        this.cols = new ArrayList<>();
    }

    void print(){
        System.out.print("Var index: " + String.format("%2d", this.index) + "| covering rel(s): ");
        for(int i=0; i<this.covers.size(); i++){
//            System.out.print(this.cols.get(i) + ", ");
            System.out.print(this.covers.get(i).alias + "~" + this.cols.get(i) + ", ");
        }
        System.out.println();
    }

    @Override
    public String toString(){
        return Integer.toString(this.index);
    }
}
