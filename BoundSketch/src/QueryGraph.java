import net.sf.jsqlparser.expression.operators.arithmetic.Modulo;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;

import org.paukov.combinatorics3.Generator;

import javax.xml.bind.attachment.AttachmentMarshaller;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import java.util.*;

public class QueryGraph {
    private String dbName;
    private String queryID;
    private int buckets;
    private String key;
    private HashMap<Relation, String> relToName;
    private HashMap<String, Relation> aliasToRel;
    private HashMap<String, Attribute> colToAttribute;
    private int joinAttrIndexBuilder = 0;
    private ArrayList<Attribute> joinAttributes;
    private Relation[] relations;
    private ArrayList<HashMap<Relation, ArrayList<Attribute>>> relToCoveredAttributes;
    private ArrayList<Relation[]> joinAttributeCovers;
    private ArrayList<BoundFormula> boundFormulae;
    private boolean empty;

    QueryGraph(String _dbName, String _queryID, int _buckets) {
        this.dbName = _dbName;
        this.queryID = _queryID;
        this.buckets = _buckets;
        this.relToName = new HashMap<>();
        this.aliasToRel = new HashMap<>();
        this.colToAttribute = new HashMap<>();
        this.joinAttributes = new ArrayList<>();
        this.joinAttributeCovers = new ArrayList<>();
        this.boundFormulae = new ArrayList<>();

    }

    void setup(String query) {
        Expression exprW;
        Statement statement;
        Select selectStatement;
        PlainSelect plainSelect;
        try {
            statement = CCJSqlParserUtil.parse(query);
            selectStatement = (Select) statement;
            plainSelect = (PlainSelect) selectStatement.getSelectBody();

            this.getRelations(plainSelect);
            exprW = plainSelect.getWhere();
            this.getAttributes(exprW);
        } catch (JSQLParserException e) {
            System.out.println(query);
            e.printStackTrace();
            System.exit(0);
        }
    }

    void process(BoundIndexBuilder bib,
                 HashMap<String, Sketch> sketchMap,
                 HashMap<String, Long> filterCountMap,
                 HashMap <String, ArrayList<Long>> filterIDsMap,
                 ThreadPoolExecutor executor) {
        this.identifyKeys();
        this.propagateFilters(filterCountMap, filterIDsMap);
        if(!this.empty) {
            this.eliminateFKSatellites();
            this.compactVarIndices();
            this.getJoinAttributeCovers();
            this.getBoundFormulae(bib, sketchMap, executor);
        }
    }

//    void processTiming(BoundIndexBuilder bib,
//                       HashMap<String, Sketch> sketchMap,
//                       HashMap<String, Long> filterCountMap,
//                       HashMap <String, ArrayList<Long>> filterIDsMap,
//                       ThreadPoolExecutor executor,
//                       Timer timer) {
//        timer.start("keys");
//        this.identifyKeys();
//        timer.log();
//
//
//        this.propagateFiltersTiming(filterCountMap, filterIDsMap, timer);
//        if(!this.empty) {
//            timer.start("fksat");
//            this.eliminateFKSatellites();
//            timer.log();
//
//            timer.start("compact");
//            this.compactVarIndices();
//            timer.log();
//
//            timer.start("covers");
//            this.getJoinAttributeCovers();
//            timer.log();
//
//            this.getBoundFormulaeTiming(bib, sketchMap, executor, timer);
//        }
//    }

    private void getRelations(PlainSelect plainSelect) {
        String full = plainSelect.getFromItem().toString();
        String[] split = full.split("AS");
        Relation r = new Relation(split[1].trim());
        this.relToName.put(r, split[0].trim());
        r.tableName = split[0].trim();
        this.aliasToRel.put(split[1].trim(), r);

        if (plainSelect.getJoins() != null) {
            for (Join j : plainSelect.getJoins()) {
                full = j.getRightItem().toString();
                split = full.split("AS");
                r = new Relation(split[1].trim());
                this.relToName.put(r, split[0].trim());
                r.tableName = split[0].trim();
                this.aliasToRel.put(r.alias, r);
            }
        }

        String[] allTableNames = this.relToName.values().toArray(new String[this.relToName.size()]);
        Arrays.sort(allTableNames);
        StringBuilder sb = new StringBuilder();
        for (String tableName : allTableNames) {
            sb.append(tableName);
            sb.append(",");
        }
        sb.append(":");
        this.key = sb.toString();
    }

    private void getAttributes(Expression expr) {
        Column lCol;
        Column rCol;

        if (expr instanceof AndExpression) {
            this.getAttributes(((AndExpression) expr).getLeftExpression());
            this.getAttributes(((AndExpression) expr).getRightExpression());
        } else if (expr instanceof EqualsTo) {
            /* either an equivalence selection predicate or a join predicate */
            EqualsTo equalsToExpr = (EqualsTo) expr;

            /* join predicate */
            if (equalsToExpr.getLeftExpression() instanceof Column &&
                    equalsToExpr.getRightExpression() instanceof Column) {
                lCol = (Column) equalsToExpr.getLeftExpression();
                rCol = (Column) equalsToExpr.getRightExpression();

                this.processJoinPredicate(lCol, rCol);
            }
            /* equivalence selection predicate */
            else {
                this.newFilter(expr);
            }
        } else {
            this.newFilter(expr);
        }
    }

    private void newFilter(Expression expr) {
        Filter f = new Filter(expr);
        String alias = this.getAlias(expr);

        this.aliasToRel.get(alias).filters.add(f);
    }

    private String getAlias(Expression expr) {
        if (expr instanceof BinaryExpression) {
            BinaryExpression binaryExpr = (BinaryExpression) expr;
            if (binaryExpr.getLeftExpression() instanceof Column) {
                Column lCol = (Column) binaryExpr.getLeftExpression();
                return lCol.getTable().toString();
            }
            else if(binaryExpr instanceof OrExpression){
                return this.getAlias(binaryExpr.getLeftExpression());
            }
            else if(binaryExpr instanceof EqualsTo){
                return this.getAlias(binaryExpr.getLeftExpression());
            }
            else if(binaryExpr instanceof Modulo){
                return this.getAlias(binaryExpr.getLeftExpression());
            }
        }
        else if(expr instanceof InExpression) {
            InExpression inExpr = (InExpression) expr;
            if (inExpr.getLeftExpression() instanceof Column) {
                Column lCol = (Column) inExpr.getLeftExpression();
                return lCol.getTable().toString();
            }
        }
        else if(expr instanceof Parenthesis){
            Parenthesis parenthesis = (Parenthesis) expr;
            return this.getAlias(parenthesis.getExpression());
        }
        else if(expr instanceof OrExpression){
            OrExpression orExpr = (OrExpression) expr;
            return this.getAlias(orExpr.getLeftExpression());
        }
        else if(expr instanceof IsNullExpression){
            IsNullExpression isNullExpr = (IsNullExpression) expr;
            if (isNullExpr.getLeftExpression() instanceof Column) {
                Column lCol = (Column) isNullExpr.getLeftExpression();
                return lCol.getTable().toString();
            }
        }
        else if(expr instanceof Between){
            Between between = (Between) expr;
            if (between.getLeftExpression() instanceof Column) {
                Column lCol = (Column) between.getLeftExpression();
                return lCol.getTable().toString();
            }
        }
        else if(expr instanceof Modulo){
            Modulo modulo = (Modulo) expr;
            if (modulo.getLeftExpression() instanceof Column) {
                Column lCol = (Column) modulo.getLeftExpression();
                return lCol.getTable().toString();
            }
        }

        return null;
    }

    private void processJoinPredicate(Column lCol,
                                      Column rCol) {
        /* brand new join attribute */
        if (this.colToAttribute.get(lCol.toString()) == null
                && this.colToAttribute.get(rCol.toString()) == null) {
            Attribute a = new Attribute(this.joinAttrIndexBuilder);
            this.joinAttrIndexBuilder++;

            a.cols.add(lCol.toString());
            a.cols.add(rCol.toString());
            a.covers.add(aliasToRel.get(lCol.getTable().toString()));
            a.covers.add(aliasToRel.get(rCol.getTable().toString()));

            aliasToRel.get(lCol.getTable().toString()).joinAttributes.add(a);
            aliasToRel.get(rCol.getTable().toString()).joinAttributes.add(a);

            this.joinAttributes.add(a);
            this.colToAttribute.put(lCol.toString(), a);
            this.colToAttribute.put(rCol.toString(), a);
        }
        /* left column is new */
        else if (this.colToAttribute.get(lCol.toString()) == null) {
            Attribute a = this.colToAttribute.get(rCol.toString());

            aliasToRel.get(lCol.getTable().toString()).joinAttributes.add(a);

            a.cols.add(lCol.toString());
            a.covers.add(aliasToRel.get(lCol.getTable().toString()));
            this.colToAttribute.put(lCol.toString(), a);
        }
        /* right column is new */
        else if (this.colToAttribute.get(rCol.toString()) == null) {
            Attribute a = this.colToAttribute.get(lCol.toString());

            aliasToRel.get(rCol.getTable().toString()).joinAttributes.add(a);

            a.cols.add(rCol.toString());
            a.covers.add(aliasToRel.get(rCol.getTable().toString()));
            this.colToAttribute.put(rCol.toString(), a);
        }
        /* both columns exist already and the joinAttributes need to be merged */
        else {
            Attribute aL = this.colToAttribute.get(lCol.toString());
            Attribute aR = this.colToAttribute.get(rCol.toString());

            /* if columns already in same equivalence class don't need to do anything */
            if(aL != aR){
                aL.cols.addAll(aR.cols);
                aL.covers.addAll(aR.covers);

                for (String col : aR.cols) {
                    this.colToAttribute.put(col, aL);
                }
            }
        }
    }

    private void identifyKeys(){
        for(Attribute a: this.joinAttributes){
            for(String col: a.cols){
                String[] colSplit = col.split("\\.");
                if(colSplit[1].equals("id")){
                    Relation r = this.aliasToRel.get(colSplit[0]);
                    r.keys.add(a);
                }
            }
        }
    }

    private void propagateFilters(HashMap<String, Long> filterCountMap,
                                        HashMap <String, ArrayList<Long>> filterIDsMap){
        String propCmdSelectCount = "SELECT COUNT(%s)\nFROM\n    %s AS %s\nWHERE\n    ";
        String propCmdSelect = "SELECT %s\nFROM\n    %s AS %s\nWHERE\n    ";

        Queue<Relation> q = new LinkedList<>();
        q.addAll(this.aliasToRel.values());

        Filter f;
        Relation r;

        while(!q.isEmpty()) {
            r = q.poll();

            /* check if table is open to selection propagation */
            if (r.filters.size() == 0 || r.keys.size() != 1) continue;
            Attribute key = r.keys.get(0);
            if (key.covers.size() > 2) continue;

            StringBuilder queryBuilder = new StringBuilder(String.format(
                    propCmdSelectCount,
                    key.cols.get(key.covers.indexOf(r)),
                    r.tableName, r.alias));

            for (int i = 0; i < r.filters.size(); i++) {
                f = r.filters.get(i);
                if (i > 0) {
                    queryBuilder.append("\n    AND ");
                }
                queryBuilder.append(f.getPredicate());
            }
            queryBuilder.append("\n;");

            String query = queryBuilder.toString();

            long count = 0;
            if(filterCountMap.get(query) != null){
                count = filterCountMap.get(query);
            }
            else {
                try {
                    java.sql.ResultSet rs = this.executeSQLQuery(query, this.dbName);
                    rs.next();
                    count = rs.getLong(1);
                    filterCountMap.put(query, count);
                } catch (SQLException e) {
                    System.out.println(query);
                    e.printStackTrace();
                    System.exit(-1);
                }
            }
            if (count > Math.pow(this.buckets, 0.5)) continue;
            if (count == 0) {
                this.empty = true;
                continue;
            } else {
                this.empty = false;
            }

            queryBuilder = new StringBuilder(String.format(
                    propCmdSelect,
                    key.cols.get(key.covers.indexOf(r)),
                    r.tableName,
                    r.alias));

            for (int i = 0; i < r.filters.size(); i++) {
                f = r.filters.get(i);
                if (i > 0) {
                    queryBuilder.append("\n    AND ");
                }
                queryBuilder.append(f.getPredicate());
            }
            queryBuilder.append("\n;");

            query = queryBuilder.toString();

            ArrayList<Long> propagatedIds = new ArrayList<>();
            if(filterIDsMap.get(query) != null){
                propagatedIds = filterIDsMap.get(query);
            }
            else {
                try {
                    java.sql.ResultSet rs = this.executeSQLQuery(query, this.dbName);
                    while (rs.next()) {
                        propagatedIds.add(rs.getLong(1));
                    }
                    filterIDsMap.put(query, propagatedIds);
                } catch (SQLException e) {
                    System.out.println(query);
                    e.printStackTrace();
                }
            }

            /* update the key Variable */
            key.cols.remove(key.covers.indexOf(r));
            key.covers.remove(r);

            /* destroy the locally know join Variables */
            this.joinAttributes.remove(key);

            /* update the newly propagated selection variable on the remaining covering relations */
            for (Relation fkR : key.covers) {
                if (!fkR.joinAttributes.contains(key)) {
                    throw new java.lang.Error("foreign key attribute not found!");
                }

                StringBuilder predicateBuilder = new StringBuilder(key.cols.get(key.covers.indexOf(fkR)) + " IN (");
                long id;
                for(int i=0; i<propagatedIds.size(); i++){
                    id = propagatedIds.get(i);
                    if(i > 0){
                        predicateBuilder.append(", ");
                    }
                    predicateBuilder.append(id);
                }
                predicateBuilder.append(")");


                fkR.joinAttributes.remove(key);
                fkR.filters.add(new Filter(predicateBuilder.toString()));

                if(!q.contains(fkR)){
                    q.add(fkR);
                }
            }

            /* destroy locally know r */
            this.relToName.remove(r);
            this.aliasToRel.remove(r.alias);
        }

        this.relations = new Relation[this.aliasToRel.size()];
        this.aliasToRel.values().toArray(this.relations);
    }

    private void eliminateFKSatellites(){
        boolean modified = true;
        ArrayList<Relation> relsToDestroy;
        while(modified) {
            relsToDestroy = new ArrayList<>();
            modified = false;
            for (Relation r : this.aliasToRel.values()) {
                /* check if relation is a key foreign key satellite */
                if (r.keys.size() != 1 || r.filters.size() > 0 || r.joinAttributes.size() != 1) continue;
                Attribute key = r.keys.get(0);

                relsToDestroy.add(r);
                modified = true;

                /* remove reference to current table from covers of key Variable along with the column name */
                key.cols.remove(key.covers.indexOf(r));
                key.covers.remove(r);

                /* destroy the join Variables if only 1 other table uses it */
                if(key.covers.size() == 1){
                    /* destroy query graph reference */
                    this.joinAttributes.remove(key);

                    /* destroy the reference to join variable in the other relation */
                    Relation fKR = key.covers.get(0);
                    if (!fKR.joinAttributes.contains(key)) {
                        throw new java.lang.Error("foreign key attribute not found!");
                    }
                    fKR.joinAttributes.remove(key);
                }
            }

            /* destroy the foreign key satellite relations */
            for(Relation r: relsToDestroy){
                this.relToName.remove(r);
                this.aliasToRel.remove(r.alias);
            }
        }

        this.relations = new Relation[this.aliasToRel.size()];
        this.aliasToRel.values().toArray(this.relations);
    }

    private void compactVarIndices(){
        int curr = 0;
        for(Attribute a: this.joinAttributes){
            a.index = curr;
            curr++;
        }
    }

    private void getJoinAttributeCovers(){
        Relation[] rArr = new Relation[this.joinAttributes.size()];
        this.getJoinAttributeCovers(rArr, 0);

        this.relToCoveredAttributes = new ArrayList<>();
        for(Relation[] joinVarCover: this.joinAttributeCovers){
            boolean safe = true;
            HashMap<Relation, ArrayList<Attribute>> joinVarCoverMap = new HashMap<>();
            for(Relation r: this.relations){
                ArrayList<Attribute> responsibilities = new ArrayList<>();
                for(int i=0; i<this.joinAttributes.size(); i++){
                    if(joinVarCover[i] == r){
                        responsibilities.add(this.joinAttributes.get(i));
                    }
                }
                /* not technically true but works for cast_info in imdb dataset */
                if(r.joinAttributes.size() >= 2 && responsibilities.size() == 0){
                    safe = false;
                    continue;
                }

                joinVarCoverMap.put(r, responsibilities);
            }

            if(safe) this.relToCoveredAttributes.add(joinVarCoverMap);
        }
    }

    private void getJoinAttributeCovers(Relation[] relArr,
                                        int position){
        if(position == relArr.length){
            boolean safe = true;
            for(Relation r: this.relations){
                int count = 0;
                for(Relation rr: relArr){
                    if(r == rr) count++;
                }

                if(count > 0 && count != r.joinAttributes.size() && count != r.joinAttributes.size() - 1){
                    safe = false;
                    break;
                }

            }
            if(safe) this.joinAttributeCovers.add(relArr);

        }
        else{
            for(Relation r: this.joinAttributes.get(position).covers){
                Relation[] relArrNew = relArr.clone();
                relArrNew[position] = r;
                this.getJoinAttributeCovers(relArrNew, position+1);
            }
        }
    }

    private void getBoundFormulae(BoundIndexBuilder bib,
                                        HashMap<String, Sketch> sketchMap,
                                        ThreadPoolExecutor executor){
        for(HashMap<Relation, ArrayList<Attribute>> map: this.relToCoveredAttributes) {
            ArrayList<Sketch> uncL = new ArrayList<>();
            ArrayList<Sketch> conL = new ArrayList<>();
            ArrayList<Integer> activeConL = new ArrayList<>();

//            if (this.joinAttributes.size() == 0) {
//                /* this case should only arise if every other table was propagated into a single one */
//                ZeroDimensionalSketchUnc s = new ZeroDimensionalSketchUnc(
//                        this.relToName.get(this.relations[0]),
//                        null,
//                        new String[]{this.relations[0].alias + ".id"},
//                        this.dbName,
//                        new int[]{1},
//                        "deserialize",
//                        this.relations[0],
//                        executor);
//
//                s.l2gIndex.put(bib.curr, new HashMap<>());
//                s.g2lIndex.put(bib.curr, new HashMap<>());
//
//                uncL.add(s);
//                BoundFormula b = new BoundFormula(uncL.toArray(new Sketch[uncL.size()]),
//                        conL.toArray(new Sketch[conL.size()]),
//                        activeConL.toArray(new Integer[conL.size()]),
//                        new int[]{1});
//
//                b.index = bib.curr;
//                bib.curr++;
//
//                this.boundFormulae.add(b);
//
//            }
//            else {
                /* generate hash sizes for each attribute */
                HashMap<Attribute, Boolean> partioned = new HashMap<>();
                ArrayList<Relation> unconditionals = new ArrayList<>();
                for(Relation r: this.relations){
                    if (map.get(r).size() == r.joinAttributes.size()) {
                        unconditionals.add(r);
                    }
                }

                boolean coveredByUnc, coveredByCon;
                int numPartitioned = 0;
                for(Attribute a: this.joinAttributes){
                    coveredByUnc = false;
                    coveredByCon = false;

                    for(Relation r: a.covers){
                        if(unconditionals.contains(r)) {
                            coveredByUnc = true;
                        }
                        else{
                            coveredByCon = true;
                        }
                    }

                    if(coveredByUnc && coveredByCon){
                        partioned.put(a, true);
                        numPartitioned++;
                    }
                    else{
                        partioned.put(a, false);
                    }
                }

                HashMap<Attribute, Integer> hashSizesMap = new HashMap<>();
                for(Attribute a: this.joinAttributes){
                    if(partioned.get(a)){
                        hashSizesMap.put(a, (int) Math.pow(this.buckets, 1.0/numPartitioned));
                    }
                    else{
                        hashSizesMap.put(a, 1);
                    }
                }

                /* generate sketches for each relation in the join */
                for (Relation r : this.relations) {
                    Sketch s = null;

                    numPartitioned = 0;

                    for(Attribute a: r.joinAttributes){
//                        if(hashSizesMap.get(a) > 1 || this.buckets == 1) {
                        if(hashSizesMap.get(a) > 1) {
                            numPartitioned++;
                        }
                    }

                    /* generate array of column names and array of actual attributes */
                    String[] joinCols = new String[numPartitioned];
                    Attribute[] joinAttrsSpecific = new Attribute[numPartitioned];
                    int ii = 0;
                    for(Attribute aa: r.joinAttributes){
                        if(hashSizesMap.get(aa) > 1){
                            joinCols[ii] = aa.cols.get(aa.covers.indexOf(r));
                            joinAttrsSpecific[ii] = aa;
                            ii++;
                        }
                    }

                    /* get the active column (if none (unconditional) this is null) */
                    String activeCol = null;
                    Attribute activeAttribute = null;
                    if(r.joinAttributes.size() != map.get(r).size()){
                        for(Attribute a: r.joinAttributes){
                            if(!map.get(r).contains(a)){
                                activeCol = a.cols.get(a.covers.indexOf(r));
                                activeAttribute = a;
                                break;
                            }
                        }
                    }

                    /* build a probe to see if we already saw this particular sketch from another subgraph */
                    StringBuilder probeBuilder = new StringBuilder();
                    probeBuilder.append(r.alias);
                    probeBuilder.append("[");
                    probeBuilder.append(activeCol);
                    probeBuilder.append("][");
                    for(Attribute ja: joinAttrsSpecific){
                        probeBuilder.append(hashSizesMap.get(ja));
                        probeBuilder.append(", ");
                    }
                    probeBuilder.append("][");
                    for(String ja: joinCols){
                        probeBuilder.append(ja);
                        probeBuilder.append(", ");
                    }
                    probeBuilder.append("][");
                    for(Filter f: r.filters){
                        probeBuilder.append(f.getPredicate());
                        probeBuilder.append(",");
                    }
                    probeBuilder.append("]");
                    String probe = probeBuilder.toString();

                    if(sketchMap.get(probe) != null){
                        s = sketchMap.get(probe);
                    }
                    else {
                        int[] hashSizes = new int[joinAttrsSpecific.length];
                        for(int i=0; i<joinAttrsSpecific.length; i++){
                            hashSizes[i] = hashSizesMap.get(joinAttrsSpecific[i]);
                        }

                        if (joinAttrsSpecific.length == 0) {
                            if(activeCol == null) {
                                s = new ZeroDimensionalSketchUnc(
                                        this.relToName.get(r),
                                        activeCol,
                                        new String[]{r.alias + ".id"},
                                        this.dbName,
                                        new int[]{1},
                                        "deserialize",
                                        r,
                                        executor);
                                sketchMap.put(probe, s);
                            }
                            else {
                                s = new ZeroDimensionalSketchCon(
                                        this.relToName.get(r),
                                        activeCol,
                                        new String[]{r.alias + ".id"},
                                        this.dbName,
                                        new int[]{1},
                                        "deserialize",
                                        r,
                                        executor);
                                sketchMap.put(probe, s);
                            }
                        }
                        else if (joinAttrsSpecific.length == 1) {
                            if(activeCol == null) {
                                s = new OneDimensionalSketchUnc(
                                        this.relToName.get(r),
                                        activeCol,
                                        joinCols,
                                        this.dbName,
                                        hashSizes,
                                        "deserialize",
                                        r,
                                        executor);
                                sketchMap.put(probe, s);
                            }
                            else{
                                s = new OneDimensionalSketchCon(
                                        this.relToName.get(r),
                                        activeCol,
                                        joinCols,
                                        this.dbName,
                                        hashSizes,
                                        "deserialize",
                                        r,
                                        executor);
                                sketchMap.put(probe, s);
                            }
                        }
                        else if (joinAttrsSpecific.length == 2) {
                            if(activeCol == null){
                                s = new TwoDimensionalSketchUnc(
                                        this.relToName.get(r),
                                        activeCol,
                                        joinCols,
                                        this.dbName,
                                        hashSizes,
                                        "deserialize",
                                        r,
                                        executor);
                                sketchMap.put(probe, s);
                            }
                            else {
                                s = new TwoDimensionalSketchCon(
                                        this.relToName.get(r),
                                        activeCol,
                                        joinCols,
                                        this.dbName,
                                        hashSizes,
                                        "deserialize",
                                        r,
                                        executor);
                                sketchMap.put(probe, s);
                            }
                        }
                        else {
                            System.out.println("you're asking for too many attributes...");
                            System.out.println(joinAttrsSpecific.length);
                            System.out.println(Arrays.toString(joinCols));
                            System.out.println(activeCol);
                            System.exit(-1);
                        }
                    }

                    s.l2gIndex.put(bib.curr, new HashMap<>());
                    s.g2lIndex.put(bib.curr, new HashMap<>());

                    for (int i = 0; i < joinAttrsSpecific.length; i++) {
                        s.l2gIndex.get(bib.curr).put(i, joinAttrsSpecific[i].index);
                        s.g2lIndex.get(bib.curr).put(joinAttrsSpecific[i].index, i);
                    }
                    /* make sure the active attribute appears in the g2l and l2g maps */
                    try {
                        if (joinAttrsSpecific.length == 0 && activeAttribute != null) {
                            s.l2gIndex.get(bib.curr).put(0, activeAttribute.index);
                            s.g2lIndex.get(bib.curr).put(activeAttribute.index, 0);
                        }
                    }
                    catch(NullPointerException e){
                        e.printStackTrace();
                        System.out.println(activeAttribute);
                        System.out.println(bib.curr);
                        System.out.println(s.l2gIndex.toString());
                        System.exit(-1);
                    }

                    if (map.get(r).size() == r.joinAttributes.size()) {
                        uncL.add(s);
                    } else if (map.get(r).size() == r.joinAttributes.size() - 1) {
                        for(Attribute a2: r.joinAttributes) {
                            if (!map.get(r).contains(a2)) {
                                activeConL.add(a2.index);
                            }
                        }
                        conL.add(s);
                    } else {
                        System.out.println("something has gone terribly wrong...");
                    }
                }

                int[] hashSizesGlobal = new int[this.joinAttributes.size()];
                for(int i=0; i<this.joinAttributes.size(); i++){
                    hashSizesGlobal[i] = hashSizesMap.get(this.joinAttributes.get(i));
                }

                BoundFormula b = new BoundFormula(uncL.toArray(new Sketch[uncL.size()]),
                        conL.toArray(new Sketch[conL.size()]),
                        activeConL.toArray(new Integer[conL.size()]),
                        hashSizesGlobal);
                b.index = bib.curr;
                bib.curr++;

                this.boundFormulae.add(b);
            }
//        }
    }

    void getSubgraphs(){
        ArrayList<String> subgraphKeys = new ArrayList<>();
        ArrayList<Long> subgraphBounds = new ArrayList<>();
        ArrayList<QueryGraph> querySubgraphs = new ArrayList<>();

        Relation[] baseRels = new Relation[this.aliasToRel.size()];
        this.aliasToRel.values().toArray(baseRels);

        Iterator<List<Relation>> permutationIterator = Generator
                .subset(baseRels)
                .simple()
                .iterator();

        BoundIndexBuilder bib = new BoundIndexBuilder(0);
        HashMap <String, Sketch> sketchMap = new HashMap<>();
        HashMap <String, Long> boundMap = new HashMap<>();
        HashMap <String, Long> filterCountMap = new HashMap<>();
        HashMap <String, ArrayList<Long>> filterIDsMap = new HashMap<>();
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(4);

        List<Relation> rList;

        while(permutationIterator.hasNext()){
            rList = permutationIterator.next();

            /* we only care about join queries and won't write an entry in info.txt for single relation sub-queries */
            if(rList.size() <= 1) continue;

            HashMap<Relation, Boolean> allRels = new HashMap<>();
            for(Relation r: rList){
                allRels.put(r, true);
            }
            int numRels = allRels.size();

            Queue<Relation> relQ = new LinkedList<>();
            relQ.add(rList.get(0));

            HashMap<Relation, Boolean> coveredRels = new HashMap<>();
            coveredRels.put(rList.get(0), true);

            boolean connected = false;
            /* perform a DFS on the subhypergraph to check if the sughypergraph is connected */
            Relation currR;
            while(!relQ.isEmpty()){
                currR = relQ.poll();
                for(Attribute currA: currR.joinAttributes){
                    for(Relation neighborR: currA.covers){
                        if(allRels.get(neighborR) != null && coveredRels.get(neighborR) == null){
                            relQ.add(neighborR);
                            coveredRels.put(neighborR, true);
                        }
                    }
                }

                if(coveredRels.size() == numRels){
                    connected = true;
                    break;
                }
            }

            /* only generate a new query graph in case it is connected */
            if(!connected) continue;

            /* create the subhypergraph */
            QueryGraph qg = new QueryGraph(this.dbName, this.queryID, this.buckets);
            HashMap<Relation, Relation> relCloneMap = new HashMap<>();
            HashMap<Attribute, Attribute> varCloneMap = new HashMap<>();

            /* instantiate the relations */
            qg.relations = new Relation[rList.size()];
            for(int i=0; i<rList.size(); i++){
                Relation r2 = new Relation(rList.get(i).alias);
                r2.tableName = rList.get(i).tableName;
                relCloneMap.put(rList.get(i), r2);
                qg.relations[i] = r2;
            }

            /* instantiate the join attributes and setup join variable class attributes */
            HashMap<Attribute, Integer> attributeCoverCount = new HashMap<>();
            for(Relation r: rList){
                for(Attribute a: r.joinAttributes) {
                    if (attributeCoverCount.get(a) == null) attributeCoverCount.put(a, 1);
                    else attributeCoverCount.put(a, attributeCoverCount.get(a) + 1);
                }
            }
            for(Attribute a: attributeCoverCount.keySet()){
                if(attributeCoverCount.get(a) > 1){
                    Attribute a2 = new Attribute(a.index);
                    for(int i=0; i<a.covers.size(); i++){
                        if(relCloneMap.get(a.covers.get(i)) != null){
                            a2.covers.add(relCloneMap.get(a.covers.get(i)));
                            a2.cols.add(a.cols.get(i));
                        }
                    }

                    varCloneMap.put(a, a2);
                    qg.joinAttributes.add(a2);
                }
            }

            /* instantiate the filters */
            for(Relation r: rList){
                /* deep copy of the filters */
                relCloneMap.get(r).filters = new ArrayList<>();
                for(Filter f: r.filters){
                    relCloneMap.get(r).filters.add(f);
                }
            }

            /* finish up Relation class attributes (note filters have already been added) */
            for(Relation r: rList){
                for(Attribute a: r.joinAttributes){
                    if(varCloneMap.get(a) != null){
                        relCloneMap.get(r).joinAttributes.add(varCloneMap.get(a));
                    }
                }
            }

            /* copy over knowledge of the keys */
            for(Relation r: rList){
                for(Attribute a: r.keys){
                    /* this will only join variables (we assume there are no selections on key attributes) */
                    if(varCloneMap.get(a) != null){
                        relCloneMap.get(r).keys.add(varCloneMap.get(a));
                    }
                }
            }

            /* finish instantiating QueryGraph class attributes */

            /* relToName */
            for(Relation r: rList){
                qg.relToName.put(relCloneMap.get(r), this.relToName.get(r));
            }

            /* aliasToRel */
            for(Relation r: rList){
                qg.aliasToRel.put(r.alias, relCloneMap.get(r));
            }

            /* attrToVar (only used in initial variable building) */
            qg.colToAttribute = null;

            /* attributeIndexBuilder */
            qg.joinAttrIndexBuilder = this.joinAttrIndexBuilder;

            /* joinVarCovers (already set up and only used later) */
            /* relToCoveredVars (already set up and only used later) */
            /* relations (only created after propagating selections) */
            /* boundFormulae (already set up and only used later) */

            /* contruct the probe key for info.txt */
            String[] allTableAliases = new String[qg.relToName.keySet().size()];
            int i = 0;
            for(Relation r: qg.relToName.keySet()) {
                allTableAliases[i] = r.alias;
                i++;
            }
            Arrays.sort(allTableAliases);
            StringBuilder sb = new StringBuilder();
            for(String tableName: allTableAliases){
                sb.append(tableName);
                sb.append(",");
            }
            sb.append(":");
            qg.key = sb.toString();


            qg.process(bib, sketchMap, filterCountMap, filterIDsMap, executor);

            querySubgraphs.add(qg);
        }

        try {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.MINUTES);
        }
        catch(InterruptedException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        int[] uniqueSubgraphsCount = new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

        for(QueryGraph qg: querySubgraphs){
            String boundKey = qg.getBoundKey();

            long bound;
            if(boundMap.get(boundKey) == null){
                bound = qg.getBound();
                boundMap.put(boundKey, bound);

                uniqueSubgraphsCount[qg.joinAttributes.size()]++;
            }
            else{
                bound = boundMap.get(boundKey);
            }

            subgraphKeys.add(qg.key);
            subgraphBounds.add(bound);
        }
        System.out.println("core subgraph join attribute count histogram: " + Arrays.toString(uniqueSubgraphsCount));

        try {
            // PrintWriter allInfoWriter = new PrintWriter(new FileOutputStream(
            //         new File("all_info.txt"),
            //         true));
            // allInfoWriter.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
            // allInfoWriter.println("CURRENT QUERY: " + this.queryID);

            PrintWriter infoWriter = new PrintWriter("info.txt");
            for(int i=0; i<subgraphKeys.size(); i++){
                infoWriter.println(subgraphKeys.get(i) + subgraphBounds.get(i));
                // allInfoWriter.println(subgraphKeys.get(i) + subgraphBounds.get(i));
            }

            infoWriter.close();
            // allInfoWriter.close();
        }
        catch(FileNotFoundException e){
            e.printStackTrace();
            System.exit(-1);
        }
    }

    String getBoundKey(){
        StringBuilder sb = new StringBuilder();

        ArrayList<Relation> relAliasesSorted = new ArrayList<>();
        for(Relation r: this.relations){
            relAliasesSorted.add(r);
        }
        Collections.sort(relAliasesSorted, (Relation r1, Relation r2) -> r1.alias.compareTo(r2.alias));

        for(Relation r: relAliasesSorted){
            sb.append(r.alias + ":[");

            String[] joinVarsOrdered = new String[r.joinAttributes.size()];
            for(int i=0; i<r.joinAttributes.size(); i++){
                Attribute a = r.joinAttributes.get(i);
                joinVarsOrdered[i] = a.cols.get(a.covers.indexOf(r));
            }
            Arrays.sort(joinVarsOrdered);

            for(String s: joinVarsOrdered){
                sb.append(s + ",");
            }

            sb.append("]:[");

            String[] filtersOrdered = new String[r.filters.size()];
            for(int i=0; i<r.filters.size(); i++){
                filtersOrdered[i] = r.filters.get(i).getPredicate();
            }
            Arrays.sort(filtersOrdered);

            for(int i=0; i<filtersOrdered.length; i++){
                sb.append(filtersOrdered[i] + ",");
            }

            sb.append("]");
        }
        return(sb.toString());
    }

    long getBound(){
        if(this.empty){
            return 0;
        }

        if(this.joinAttributes.size() == 0) {
            return Arrays.stream(((ZeroDimensionalSketchUnc) this.boundFormulae.get(0).uncList[0]).unc).sum();
        }
        else {
            long[] bounds = new long[this.boundFormulae.size()];
            BoundFormula bf;
            Iterator<int[]> cp;
            int[] index;
            for(int i=0; i<this.boundFormulae.size(); i++){
                bf = this.boundFormulae.get(i);
                cp = new CrossProduct(bf.hashSizes).iterator();
                if (this.buckets > 1) {
                    while (cp.hasNext()) {
                        index = cp.next();
                        bounds[i] += bf.execute(index);
                    }
                }
                else {
                    index = new int[bf.hashSizes.length];
                    bounds[i] = bf.execute(index);
                }
            }

            for(int i=0; i<bounds.length; i++){
                if(bounds[i] < 0){
                    bounds[i] = Long.MAX_VALUE;
                }
            }

            return this.min(bounds);
        }
    }

    private long min(long[] arr) {
        if (arr.length == 0) {
            return 0;
        }
        long m = arr[0];
        for (long d: arr) {
            if (d < m) {
                m = d;
            }
        }
        return m;
    }

    private int minIndex(long[] arr) {
        if (arr.length == 0) {
            return -1;
        }
        int index = 0;
        for (int i=0; i<arr.length; i++) {
            if (arr[i] < arr[index]) {
                index = i;
            }
        }
        return index;
    }

    private java.sql.ResultSet executeSQLQuery(String command,
                                               String dbName){
        try {
            Connection conn;
            conn = DriverManager.getConnection(
                    String.format("jdbc:postgresql://127.0.0.1:5432/%s", dbName),
                    "postgres",
                    "buds");
            java.sql.Statement st;
            java.sql.ResultSet rs;

            st = conn.createStatement();
            rs = st.executeQuery(command);
            conn.close();
            return rs;
        } catch (SQLException e) {
            System.out.println("Connection/Querying Failed!");
            System.out.println(command);
            e.printStackTrace();
            return null;
        }
    }

    public void describe() {
        System.out.println("Relations:");
        for(Relation r:this.aliasToRel.values()){
            r.print();
        }
        System.out.println("Join Attributes:");
        for(Attribute a: this.joinAttributes){
            a.print();
        }
    }

    public void describeBoundFormulae(){
        System.out.println("\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\");
        for(BoundFormula bf: this.boundFormulae){
            bf.describe();
        }
        System.out.println("//////////////////////");
    }


}

