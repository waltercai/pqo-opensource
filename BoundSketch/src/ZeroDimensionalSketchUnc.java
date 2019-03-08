import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.ThreadPoolExecutor;

public class ZeroDimensionalSketchUnc extends Sketch {
    long[] unc;

    ZeroDimensionalSketchUnc(String _name,
                         String _activeCol,
                         String[] _attrs,
                         String _dbName,
                         int[] _hashSizes,
                         String method,
                         Relation r,
                         ThreadPoolExecutor executor){
        super(_name, _activeCol, _attrs, _dbName, _hashSizes);

        this.unc = new long[1];

        if(method.equals("deserialize")){
            this.deserialize(r, executor);
        }
        else {
            this.populateParallel(r, executor);
        }
    }

    public void populateParallel(Relation r,
                                 ThreadPoolExecutor executor){
        String sql =
                "SELECT\n" +
                "    COUNT(*)\n" +
                "FROM\n" +
                "   %s AS %s\n";

        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            System.out.println("PostgreSQL JDBC driver missing! Include in classpath.");
            e.printStackTrace();
            return;
        }

        Connection conn;
        StringBuilder queryBuilder;
        Filter f;

        queryBuilder = new StringBuilder(String.format(
                sql,
                r.tableName, r.alias
        ));

        if(r.filters.size() > 0){
            queryBuilder.append("WHERE\n    ");
            for (int i = 0; i < r.filters.size(); i++) {
                f = r.filters.get(i);
                if (i > 0) {
                    queryBuilder.append("\n    AND ");
                }
                queryBuilder.append(f.getPredicate());
            }
        }

        queryBuilder.append("\n;");

        String q0 = queryBuilder.toString();

        Populator p0 = new Populator(
                q0,
                r,
                this.dbName,
                this,
                0);

        executor.execute(p0);
    }

    public void printGrid(){
    }

    private void deserialize(Relation r,
                             ThreadPoolExecutor executor){
        if(r == null){
            this.populateParallel(r, executor);
        }
        else if(r.filters.size() > 0){
            this.populateParallel(r, executor);
        }
        else {
            try {
//                System.out.println(Arrays.toString(this.attrs));
                if(this.activeCol == null) {
                    this.path = String.format("%s/0d%s_[%s]_[%d]_[%s].txt",
                            this.dbName, this.name, null, this.hashSizes[0],
                            this.attrs[0].split("\\.")[1]);
                }
                else{
                    this.path = String.format("%s/0d%s_[%s]_[%d]_[%s].txt",
                            this.dbName, this.name, this.activeCol.split("\\.")[1], this.hashSizes[0],
                            this.attrs[0].split("\\.")[1]);
                }
                // FileReader reads text files in the default encoding.
                FileReader fileReader = new FileReader(this.path);

                // Always wrap FileReader in BufferedReader.
                BufferedReader bufferedReader = new BufferedReader(fileReader);

                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    String[] lineSplit = line.split(",");
                    this.unc[0] = Integer.parseInt(lineSplit[0]);
                }
                bufferedReader.close();
            } catch (FileNotFoundException e) {
                System.out.println("Unable to open file '" + this.path + "'");
                this.populateParallel(r, executor);
            } catch (IOException e) {
                System.out.println("Error reading file '" + this.path + "'");
                this.populateParallel(r, executor);
            }
        }
    }

    @Override
    public void quickDescribe(){
        System.out.println(this.name);
        System.out.println(Arrays.toString(this.attrs));
        System.out.println(Arrays.toString(this.hashSizes));
        System.out.println(this.unc.length);
        System.out.println(Arrays.toString(this.unc));
    }

    @Override
    public long access(int boundId, int gVarIndex, int[] arr) {
        try {
            return this.unc[0];
        }
        catch(Exception e){
            System.out.println("relation name: " + this.name);
            System.out.println("boundId: " + boundId);
            System.out.println("hash Sizes: " + Arrays.toString(this.hashSizes));
            System.out.println("arr: " + Arrays.toString(arr));
            System.out.println("l2g: " + this.l2gIndex.get(boundId).toString());
            System.out.println("g2l: " + this.g2lIndex.get(boundId).toString());
            e.printStackTrace();
            System.exit(-1);
        }
        return 0;
    }

    @Override
    public void serialize(Relation r){
        if(r != null) {
            if(r.filters.size() == 0) {
                try {
                    if(this.activeCol == null){
                        this.path = String.format("%s/0d%s_[%s]_[%d]_[%s].txt",
                                this.dbName, this.name, null, this.hashSizes[0],
                                this.attrs[0].split("\\.")[1]);
                    }
                    else{
                        this.path = String.format("%s/0d%s_[%s]_[%d]_[%s].txt",
                                this.dbName, this.name, this.activeCol.split("\\.")[1], this.hashSizes[0],
                                this.attrs[0].split("\\.")[1]);
                    }
                    System.out.println("serializing:        " + this.path);
                    PrintWriter pr = new PrintWriter(this.path);

                    for (int i = 0; i < this.hashSizes[0]; i++) {
                        pr.println(String.format("%d", this.unc[i]));
                    }
                    pr.close();
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("Error During Serialization.");
                }
            }
        }
        else{
            try {
                if(this.activeCol == null){
                    this.path = String.format("%s/0d%s_[%s]_[%d]_[%s].txt",
                            this.dbName, this.name, null, this.hashSizes[0],
                            this.attrs[0].split("\\.")[1]);
                }
                else{
                    this.path = String.format("%s/0d%s_[%s]_[%d]_[%s].txt",
                            this.dbName, this.name, this.activeCol.split("\\.")[1], this.hashSizes[0],
                            this.attrs[0].split("\\.")[1]);
                }
                System.out.println("serializing:        " + this.path);
                PrintWriter pr = new PrintWriter(this.path);


                for (int i = 0; i < this.hashSizes[0]; i++) {
                    pr.println(String.format("%d", this.unc[i]));
                }
                pr.close();
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Error During Serialization.");
            }
        }
    }

}
