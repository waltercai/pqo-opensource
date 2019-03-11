import java.io.PrintWriter;
import java.sql.*;
import java.io.*;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.ThreadPoolExecutor;

public class TwoDimensionalSketchCon extends Sketch {
    HashMap<Integer, long[][]> con;

    TwoDimensionalSketchCon(String _name,
                            String _activeCol,
                            String[] _attrs,
                            String _dbName,
                            int[] _hashSizes,
                            String method,
                            Relation r,
                            ThreadPoolExecutor executor){
        super(_name, _activeCol, _attrs, _dbName, _hashSizes);
        this.con = new HashMap<>();
        this.con.put(0, new long[this.hashSizes[0]][this.hashSizes[1]]);
        this.con.put(1, new long[this.hashSizes[0]][this.hashSizes[1]]);

        if(method.equals("deserialize")){
            this.deserialize(r, executor);
        }
        else {
            this.populateParallel(r, executor);
        }
    }

    private void populateParallel(Relation r,
                                  ThreadPoolExecutor executor){
        String sql2d1 =
                "SELECT\n" +
                        "    t.ha AS ha,\n" +
                        "    t.hb AS hb,\n" +
                        "    SUM(t.cnt) AS cnt,\n" +
                        "    MAX(t.cnt) AS max_degree\n" +
                        "    FROM(\n" +
                        "        SELECT\n" +
                        "            abs(hash_string(%s::text, 'murmur3', 0)%%%d) AS ha,\n" +
                        "            abs(hash_string(%s::text, 'murmur3', 0)%%%d) AS hb,\n" +
                        "            %s AS x,\n" +
                        "            COUNT(*) AS cnt\n" +
                        "        FROM %s AS %s\n";
        String sql2d2 =
                "        GROUP BY ha, hb, x) AS t\n" +
                        "GROUP BY ha, hb\n" +
                        "ORDER BY ha, hb;";

        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            System.out.println("PostgreSQL JDBC driver missing! Include in classpath.");
            e.printStackTrace();
            return;
        }

        StringBuilder queryBuilder;
        Filter f;

        queryBuilder = new StringBuilder(String.format(
                sql2d1,
                this.attrs[0], this.hashSizes[0],
                this.attrs[1], this.hashSizes[1],
                this.attrs[0],
                this.name, r.alias
        ));

        if(r.filters.size() > 0){
            queryBuilder.append("        WHERE\n            ");
            for (int i = 0; i < r.filters.size(); i++) {
                f = r.filters.get(i);
                if (i > 0) {
                    queryBuilder.append("\n            AND ");
                }
                queryBuilder.append(f.getPredicate());
            }
        }

        queryBuilder.append("\n");
        queryBuilder.append(sql2d2);

        String q0 = queryBuilder.toString();

        queryBuilder = new StringBuilder(String.format(
                sql2d1,
                this.attrs[0], this.hashSizes[0],
                this.attrs[1], this.hashSizes[1],
                this.attrs[1],
                r.tableName, r.alias
        ));

        if(r.filters.size() > 0){
            queryBuilder.append("        WHERE\n            ");
            for (int i = 0; i < r.filters.size(); i++) {
                f = r.filters.get(i);
                if (i > 0) {
                    queryBuilder.append("\n            AND ");
                }
                queryBuilder.append(f.getPredicate());
            }
        }

        queryBuilder.append("\n");
        queryBuilder.append(sql2d2);

        String q1 = queryBuilder.toString();

        Populator p0 = new Populator(
                q0,
                r,
                this.dbName,
                this,
                0);
        Populator p1 = new Populator(
                q1,
                r,
                this.dbName,
                this,
                1);

        executor.execute(p0);
        executor.execute(p1);
    }

    void printGrid(double[][] grid){
        System.out.print("[");
        for(int i = 0; i < grid.length; i++){
            for(int j = 0; j < grid[0].length; j++){
                System.out.printf("%f ", grid[i][j]);
            }
            if(i == grid[0].length - 1){
                System.out.print("]");
            }
            System.out.println();
        }
    }

    void printGrid(long[][] grid){
        System.out.print("[");
        for(int i = 0; i < grid.length; i++){
            for(int j = 0; j < grid[0].length; j++){
                System.out.printf("%d ", grid[i][j]);
            }
            if(i == grid[0].length - 1){
                System.out.print("]");
            }
            System.out.println();
        }
    }

    private void deserialize(Relation r,
                             ThreadPoolExecutor executor){
        if(r != null) {
            if (r.filters.size() > 0) {
                this.populateParallel(r, executor);
                return;
            }
            try {
                if(this.activeCol == null){
                    this.path = String.format("sketch_dir/BoundSketch/%s_sketches/2d%s_[%s]_[%d|%d]_[%s|%s].txt",
                            this.dbName, this.name, null, this.hashSizes[0], this.hashSizes[1],
                            this.attrs[0].split("\\.")[1], this.attrs[1].split("\\.")[1]);
                }
                else{
                    this.path = String.format("sketch_dir/BoundSketch/%s_sketches/2d%s_[%s]_[%d|%d]_[%s|%s].txt",
                            this.dbName, this.name, this.activeCol.split("\\.")[1], this.hashSizes[0], this.hashSizes[1],
                            this.attrs[0].split("\\.")[1], this.attrs[1].split("\\.")[1]);
                }
                // FileReader reads text files in the default encoding.
                FileReader fileReader = new FileReader(this.path);

                // Always wrap FileReader in BufferedReader.
                BufferedReader bufferedReader = new BufferedReader(fileReader);

                String line;
                int i, j;
                if(this.activeCol.equals(this.attrs[0])) {
                    while ((line = bufferedReader.readLine()) != null) {
                        String[] lineSplit = line.split(",");
                        i = Integer.parseInt(lineSplit[0]);
                        j = Integer.parseInt(lineSplit[1]);

                        this.con.get(0)[i][j] = Long.parseLong(lineSplit[2]);
                    }
                }
                else{
                    while ((line = bufferedReader.readLine()) != null) {
                        String[] lineSplit = line.split(",");
                        i = Integer.parseInt(lineSplit[0]);
                        j = Integer.parseInt(lineSplit[1]);

                        this.con.get(1)[i][j] = Long.parseLong(lineSplit[2]);
                    }
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
        else{
            try {
                if(this.activeCol == null){
                    this.path = String.format("sketch_dir/BoundSketch/%s_sketches/2d%s_[%s]_[%d|%d]_[%s|%s].txt",
                            this.dbName, this.name, null, this.hashSizes[0], this.hashSizes[1],
                            this.attrs[0].split("\\.")[1], this.attrs[1].split("\\.")[1]);
                }
                else{
                    this.path = String.format("sketch_dir/BoundSketch/%s_sketches/2d%s_[%s]_[%d|%d]_[%s|%s].txt",
                            this.dbName, this.name, this.activeCol.split("\\.")[1], this.hashSizes[0], this.hashSizes[1],
                            this.attrs[0].split("\\.")[1], this.attrs[1].split("\\.")[1]);
                }
                // FileReader reads text files in the default encoding.
                FileReader fileReader = new FileReader(this.path);

                // Always wrap FileReader in BufferedReader.
                BufferedReader bufferedReader = new BufferedReader(fileReader);

                String line;
                int i, j;
                if(this.activeCol.split(".")[1].equals(this.attrs[0])) {
                    while ((line = bufferedReader.readLine()) != null) {
                        String[] lineSplit = line.split(",");
                        i = Integer.parseInt(lineSplit[0]);
                        j = Integer.parseInt(lineSplit[1]);

                        this.con.get(0)[i][j] = Long.parseLong(lineSplit[2]);
                    }
                }
                else{
                    while ((line = bufferedReader.readLine()) != null) {
                        String[] lineSplit = line.split(",");
                        i = Integer.parseInt(lineSplit[0]);
                        j = Integer.parseInt(lineSplit[1]);

                        this.con.get(1)[i][j] = Long.parseLong(lineSplit[2]);
                    }
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

    private int getIndex(String[] arr, String s){
        for(int i=0; i<arr.length; i++){
            if(arr[i].equals(s)){
                return i;
            }
        }
        return -1;
    }

    @Override
    public void quickDescribe(){
        System.out.println(this.name);
        System.out.println(Arrays.toString(this.attrs));
        System.out.println(Arrays.toString(this.hashSizes));
    }

    @Override
    public long access(int boundId, int gVarIndex, int[] arr) {
        return this.con.get(this.g2lIndex.get(boundId).get(gVarIndex))
                [arr[this.l2gIndex.get(boundId).get(0)]]
                [arr[this.l2gIndex.get(boundId).get(1)]];
    }

    @Override
    public void serialize(Relation r){
        if(r != null) {
            if(r.filters.size() == 0) {
                try {
                    if(this.activeCol == null){
                        this.path = String.format("sketch_dir/BoundSketch/%s_sketches/2d%s_[%s]_[%d|%d]_[%s|%s].txt",
                                this.dbName, this.name, null, this.hashSizes[0], this.hashSizes[1],
                                this.attrs[0].split("\\.")[1], this.attrs[1].split("\\.")[1]);
                    }
                    else{
                        this.path = String.format("sketch_dir/BoundSketch/%s_sketches/2d%s_[%s]_[%d|%d]_[%s|%s].txt",
                                this.dbName, this.name, this.activeCol.split("\\.")[1], this.hashSizes[0], this.hashSizes[1],
                                this.attrs[0].split("\\.")[1], this.attrs[1].split("\\.")[1]);
                    }
                    System.out.println("serializing:        " + this.path);
                    PrintWriter pr = new PrintWriter(this.path);

                    if(this.activeCol.equals(this.attrs[0])) {
                        for (int i = 0; i < this.hashSizes[0]; i++) {
                            for (int j = 0; j < this.hashSizes[1]; j++) {
                                pr.println(String.format("%d,%d,%d", i, j, this.con.get(0)[i][j]));
                            }
                        }
                    }
                    else if(this.activeCol.equals(this.attrs[1])) {
                        for (int i = 0; i < this.hashSizes[0]; i++) {
                            for (int j = 0; j < this.hashSizes[1]; j++) {
                                pr.println(String.format("%d,%d,%d", i, j, this.con.get(1)[i][j]));
                            }
                        }
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
                    this.path = String.format("sketch_dir/BoundSketch/%s_sketches/2d%s_[%s]_[%d|%d]_[%s|%s].txt",
                            this.dbName, this.name, null, this.hashSizes[0], this.hashSizes[1],
                            this.attrs[0].split("\\.")[1], this.attrs[1].split("\\.")[1]);
                }
                else{
                    this.path = String.format("sketch_dir/BoundSketch/%s_sketches/2d%s_[%s]_[%d|%d]_[%s|%s].txt",
                            this.dbName, this.name, this.activeCol.split("\\.")[1], this.hashSizes[0], this.hashSizes[1],
                            this.attrs[0].split("\\.")[1], this.attrs[1].split("\\.")[1]);
                }
                System.out.println("serializing:        " + this.path);
                PrintWriter pr = new PrintWriter(this.path);

                if(this.activeCol.split(".")[1].equals(this.attrs[0])) {
                    for (int i = 0; i < this.hashSizes[0]; i++) {
                        for (int j = 0; j < this.hashSizes[1]; j++) {
                            pr.println(String.format("%d,%d,%d", i, j, this.con.get(0)[i][j]));
                        }
                    }
                }
                else if(this.activeCol.split(".")[1].equals(this.attrs[1])) {
                    for (int i = 0; i < this.hashSizes[0]; i++) {
                        for (int j = 0; j < this.hashSizes[1]; j++) {
                            pr.println(String.format("%d,%d,%d", i, j, this.con.get(1)[i][j]));
                        }
                    }
                }
                pr.close();
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Error During Serialization.");
            }
        }
    }
}
