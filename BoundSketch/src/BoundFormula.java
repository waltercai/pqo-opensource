import java.util.Arrays;
import java.util.Iterator;

public class BoundFormula {
    Sketch[] uncList;
    private Sketch[] conList;
    private Integer[] activeConL;
    int index;
    int[] hashSizes;

    BoundFormula(Sketch[] _uncList, Sketch[] _conList, Integer[] _activeConL, int[] _hashSizes){
        this.uncList = _uncList;
        this.conList = _conList;
        this.activeConL = _activeConL;
        this.hashSizes = _hashSizes;
    }

    void describe(){
        System.out.println("Bound Index: " + this.index);
        System.out.print("Unconditional Sketches: ");
        for(Sketch s: this.uncList){
            if(s instanceof ZeroDimensionalSketchUnc){
                System.out.println(s.name + " {0D}, ");
            }
            else if(s instanceof OneDimensionalSketchUnc){
                System.out.print(s.name + " {1D}, ");
            }
            else if(s instanceof TwoDimensionalSketchUnc){
                System.out.print(s.name + " {2D}, ");
            }
        }
        System.out.println();
        System.out.print("Conditional Sketches: ");
        for(int i=0; i<this.conList.length; i++){
            if(this.conList[i] instanceof OneDimensionalSketchCon){
                System.out.print(this.conList[i].name + " {0D}[" + this.activeConL[i] + "], ");
            }
            else if(this.conList[i] instanceof OneDimensionalSketchCon){
                System.out.print(this.conList[i].name + " {1D}[" + this.activeConL[i] + "], ");
            }
            else if(this.conList[i] instanceof TwoDimensionalSketchCon){
                System.out.print(this.conList[i].name + " {2D}[" + this.activeConL[i] + "], ");
            }
        }
        System.out.println();
        System.out.println("Hash Sizes");
        System.out.println(Arrays.toString(this.hashSizes));
//        System.out.println("Index: " + this.index);
    }

    long execute(int[] indices){

        long prod = 1;
        for(Sketch s: uncList){
            prod *= s.access(this.index, -1, indices);
        }
        for(int i=0; i<this.conList.length; i++){
            prod *= conList[i].access(this.index, activeConL[i], indices);
        }

//        ///////////////////////////
//        if(this.index == 33642){
//            System.out.println("Bound Formula Found!");
//            if(indices[0] == 0 && indices[1] == 0 && indices[2] == 0) {
//                for (Sketch s : uncList) {
//                    System.out.println("unc|" + s.name + ":" + s.access(this.index, -1, indices));
//                }
//                for (int i = 0; i < this.conList.length; i++) {
//                    System.out.println("con|" + conList[i].name + ":" + conList[i].access(this.index, activeConL[i], indices));
//                }
//            }
//        }
        return prod;
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("    UNCONDITIONAL SKETCHES:\n");
        for(Sketch sketch: this.uncList){
            sb.append(sketch.name);
            sb.append(":");
            sb.append(Arrays.toString(sketch.attrs));
            sb.append("\n");
        }
        sb.append("    CONDITIONAL SKETCHES:\n");
        for(int i=0; i<this.conList.length; i++){
            Sketch sketch = conList[i];

            sb.append(sketch.name);
            sb.append(":");
            sb.append(Arrays.toString(sketch.attrs));
            sb.append(":");
            sb.append(this.activeConL[i]);
            sb.append("\n");
        }

        return sb.toString();
    }
}
