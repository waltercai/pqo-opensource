import java.util.HashMap;
import java.util.Arrays;
import java.util.Set;

class Timer {
    private String currKey;
    private long currStart;
    private long fullStart;
    private long fullStop;
    private  HashMap<String, Long> timing;

    Timer(){
        this.currKey = null;
        this.fullStart = System.nanoTime();
        this.currStart = 0;
        this.timing = new HashMap<>();
    }

    void start(String key){
        this.currKey = key;
        if(this.timing.get(key) == null){
            this.timing.put(key, new Long(0));
        }
        this.currStart = System.nanoTime();
    }

    void log(){
        this.timing.put(this.currKey, this.timing.get(this.currKey) + System.nanoTime() - this.currStart);
    }

    void fullLog(){
        this.fullStop = System.nanoTime();
    }

    void report(){
        System.out.println("\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\");
        Set<String> keySet = this.timing.keySet();
        String[] keyArray = keySet.toArray(new String[keySet.size()]);
        Arrays.sort(keyArray);
        System.out.println(String.format("%4.4f: %s\n", (this.fullStop - this.fullStart) / 1000000000.0, "full"));
        for(String key: keyArray){
            System.out.println(String.format("%4.4f: %s", this.timing.get(key) / 1000000000.0, key));
        }
        System.out.println("//////////////////////");
    }
}
