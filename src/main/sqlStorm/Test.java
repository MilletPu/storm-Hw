
import lib.Table;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by NoNo on 2017-6-3.
 */
public class Test {
    public static void main(String args[]){
        Table t1 = new Table("user");
        List<String> c1 = new ArrayList<String>(); c1.add("userID"); c1.add("age");
        t1.setColumn(c1);
        List<String> row1 = new ArrayList<String>(); row1.add("001"); row1.add("14");
        t1.addRow(row1);
        t1.watchTable();

        Table t2 = new Table("score");
        List<String> c2 = new ArrayList<String>(); c2.add("userID"); c2.add("score");
        t2.setColumn(c2);
        List<String> row2 = new ArrayList<String>(); row2.add("001"); row2.add("98");
        t2.addRow(row2);
        t2.watchTable();

        String [] on = new String[1];
        on[0] = "userID";
        t1.merge(t2,on);
        on[0]="user.userID";
        t1.slice(on);
        t1.watchTable();
    }
}
