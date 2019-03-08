import java.util.Arrays;
import java.util.Iterator;

public class CrossProduct implements Iterable<int[]>{
    private int[] hashSizes;
    private int[] indices;
    private int totalBuckets;
    private int count;
    private int[] notOne;

    public CrossProduct(int[] _hashSizes) {
        this.hashSizes = _hashSizes;
        int size = this.hashSizes.length;
        this.indices = new int[size];
        this.totalBuckets = 1;
        for(int h: hashSizes){
            totalBuckets *= h;
        }
        this.count = 0;

        /* array of positions that we actually need to
           iterate through

           that is, not just hash size 1*/
        int numNotOnes = 0;
        for(int i: this.hashSizes){
            if(i!=1) numNotOnes++;
        }
        this.notOne = new int[numNotOnes];
        int currPos = 0;
        for(int i=size-1; i>-1; i--){
            if(this.hashSizes[i] > 1){
                this.notOne[currPos] = i;
                currPos++;
            }
        }

        try {
            if(this.notOne.length > 0){
                this.indices[this.notOne[0]] = -1;
            }
        }
        catch(Exception e){
            System.out.println("Hash Sizes: " + Arrays.toString(this.hashSizes));
            System.out.println("notOne: " + Arrays.toString(this.notOne));
            System.out.println("Total Buckets: " + this.totalBuckets);
            System.out.println("Count: " + this.count);
            e.printStackTrace();
            System.exit(-1);
        }
    }

    void describe(){
        System.out.println("--------------------------");
        System.out.println(Arrays.toString(this.hashSizes));
        System.out.println(totalBuckets);
    }

    @Override
    public Iterator<int[]> iterator(){
        Iterator<int[]> it = new Iterator<int[]>(){

            @Override
            public boolean hasNext() {
                return(count < totalBuckets);
            }

            @Override
            public int[] next() {
                for(int i: notOne){
                    if(indices[i] < hashSizes[i]-1){
                        indices[i]++;
                        count++;
                        break;

                    }
                    else{
                        indices[i] = 0;
                    }
                }
                return indices;
            }
        };
        return it;
    }
}
