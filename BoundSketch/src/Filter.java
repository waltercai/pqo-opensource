import net.sf.jsqlparser.expression.Expression;

public class Filter {
    private Expression expr;
    private String predicate;

    public Filter(Expression _expr){
        this.expr = _expr;
    }

    public Filter(String _predicate){
        this.predicate = _predicate;
    }

    public Filter(Filter f){
        this.expr = f.expr;
        this.predicate = f.predicate;
    }

    String getPredicate(){
        if(expr == null){
            return predicate;
        }
        else{
            return expr.toString();
        }
    }
}
