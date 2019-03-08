import java.io.*;
import java.util.ArrayList;

public class ResultSet {
    public ArrayList<Result> results;

    public ResultSet(){
        this.results = new ArrayList<>();
    }

    public ResultSet(String path){
        try {
            FileInputStream fis = new FileInputStream(path);
            ObjectInputStream ois = new ObjectInputStream(fis);
            this.results = (ArrayList<Result>) ois.readObject();
            ois.close();
            fis.close();
        } catch (IOException e){
            System.out.println("File path error during deserialization");
            e.printStackTrace();
        } catch (ClassNotFoundException e){
            System.out.println("Class error during deserialization");
            e.printStackTrace();
        }
    }

    public void addResult(Result r){
        this.results.add(r);
    }

    public void serialize(String path) {
        try {
            FileOutputStream fos = new FileOutputStream(path);
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(this.results);
            oos.close();
            fos.close();
        } catch (FileNotFoundException e) {
            System.out.println("File path error during serialization");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("File IO Error during serialization");
            e.printStackTrace();
        }
    }
}
