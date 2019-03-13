import java.sql.*;
import java.sql.ResultSet;

class Populator implements Runnable{
    String query;
    Relation r;
    String dbName;
    Sketch sketch;
    int attrIndex;

    Populator(String _query,
              Relation _r,
              String _dbName,
              Sketch _sketch,
              int _attrIndex){
        this.query = _query;
        this.r = _r;
        this.dbName = _dbName;
        this.sketch = _sketch;
        this.attrIndex = _attrIndex;
    }

    public void run(){
        if(this.sketch instanceof ZeroDimensionalSketchUnc){
            ZeroDimensionalSketchUnc ods = (ZeroDimensionalSketchUnc) this.sketch;
            try {
                Connection conn = DriverManager.getConnection(
                        String.format("jdbc:postgresql://127.0.0.1:5432/%s", this.dbName),
                        "_postgres",
                        "");
                Statement st;
                java.sql.ResultSet rs;

                st = conn.createStatement();
                rs = st.executeQuery(this.query);
                while (rs.next()) {
                    ods.unc[0] = rs.getLong(1);
                }

                rs.close();
                st.close();
                conn.close();
            } catch (SQLException e) {
                System.out.println("Connection/Querying Failed!");
                e.printStackTrace();
            }

            this.sketch.serialize(this.r);
        }
        else if(this.sketch instanceof ZeroDimensionalSketchCon){
            ZeroDimensionalSketchCon ods = (ZeroDimensionalSketchCon) this.sketch;
            try {
                Connection conn = DriverManager.getConnection(
                        String.format("jdbc:postgresql://127.0.0.1:5432/%s", this.dbName),
                        "postgres",
                        "buds");
                Statement st;
                java.sql.ResultSet rs;

                st = conn.createStatement();
                rs = st.executeQuery(this.query);
                while (rs.next()) {
                    ods.con.get(0)[0] = rs.getLong(1);
                }

                rs.close();
                st.close();
                conn.close();
            } catch (SQLException e) {
                System.out.println("Connection/Querying Failed!");
                e.printStackTrace();
            }

            this.sketch.serialize(this.r);
        }
        else if(this.sketch instanceof OneDimensionalSketchUnc){
            OneDimensionalSketchUnc ods = (OneDimensionalSketchUnc) this.sketch;
            try {
                Connection conn = DriverManager.getConnection(
                        String.format("jdbc:postgresql://127.0.0.1:5432/%s", this.dbName),
                        "postgres",
                        "buds");
                Statement st;
                java.sql.ResultSet rs;

                int ha;

                st = conn.createStatement();
                rs = st.executeQuery(this.query);
                while (rs.next()) {
                    ha = rs.getInt(1);
                    ods.unc[ha] = rs.getLong(2);
                }

                rs.close();
                st.close();
                conn.close();
            } catch (SQLException e) {
                System.out.println("Connection/Querying Failed!");
                e.printStackTrace();
            }

            this.sketch.serialize(this.r);
        }
        else if(this.sketch instanceof OneDimensionalSketchCon){
            OneDimensionalSketchCon ods = (OneDimensionalSketchCon) this.sketch;
            try {
                Connection conn = DriverManager.getConnection(
                        String.format("jdbc:postgresql://127.0.0.1:5432/%s", this.dbName),
                        "postgres",
                        "buds");
                Statement st;
                java.sql.ResultSet rs;

                int ha;

                st = conn.createStatement();
                rs = st.executeQuery(this.query);
                while (rs.next()) {
                    ha = rs.getInt(1);
                    ods.con.get(0)[ha] = rs.getLong(2);
                }

                rs.close();
                st.close();
                conn.close();
            } catch (SQLException e) {
                System.out.println("Connection/Querying Failed!");
                e.printStackTrace();
            }

            this.sketch.serialize(this.r);
        }
        else if(this.sketch instanceof TwoDimensionalSketchUnc) {
            TwoDimensionalSketchUnc tds = (TwoDimensionalSketchUnc) this.sketch;
            try {
                Connection conn = DriverManager.getConnection(
                        String.format("jdbc:postgresql://127.0.0.1:5432/%s", this.dbName),
                        "postgres",
                        "buds");
                Statement st;
                java.sql.ResultSet rs;

                int ha, hb;

                st = conn.createStatement();
                rs = st.executeQuery(this.query);
                while (rs.next()) {
                    ha = rs.getInt(1);
                    hb = rs.getInt(2);
                    tds.unc[ha][hb] = rs.getLong(3);
                }

                rs.close();
                st.close();
                conn.close();
            } catch (SQLException e) {
                System.out.println("Connection/Querying Failed!");
                e.printStackTrace();
            }

            this.sketch.serialize(this.r);
        }
        else if(this.sketch instanceof TwoDimensionalSketchCon) {
            TwoDimensionalSketchCon tds = (TwoDimensionalSketchCon) this.sketch;
            try {
                Connection conn = DriverManager.getConnection(
                        String.format("jdbc:postgresql://127.0.0.1:5432/%s", this.dbName),
                        "postgres",
                        "buds");
                Statement st;
                java.sql.ResultSet rs;

                int ha, hb;

                st = conn.createStatement();
                rs = st.executeQuery(this.query);
                while (rs.next()) {
                    ha = rs.getInt(1);
                    hb = rs.getInt(2);
                    tds.con.get(this.attrIndex)[ha][hb] = rs.getLong(4);
                }

                rs.close();
                st.close();
                conn.close();
            } catch (SQLException e) {
                System.out.println("Connection/Querying Failed!");
                e.printStackTrace();
            }

            this.sketch.serialize(this.r);
        }
    }
}