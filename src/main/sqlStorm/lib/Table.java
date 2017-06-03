package lib;
import clojure.lang.IFn;
import readers.TableReader;

import java.io.*;
import java.util.*;

/**
 * Created by NoNo on 2017-6-3.
 */
public class Table implements Serializable{
    public String tableName = "-1";
    public Map<String,Integer> column = new HashMap<String,Integer>();
    public Map<Integer,String> column_idx = new HashMap<Integer,String>();
    public Vector<List<String>> rows = new Vector<List<String>>();
    public Table(){ }
    public Table(String name){ tableName = name; }
    public void setColumn(List<String> str){
        column = new HashMap<String,Integer>();
        for(int i =0;i<str.size();i++){
            column.put(str.get(i),i);
            column_idx.put(i,str.get(i));
        }
    }
    public List<String> getColumn(){
        Set<Integer> keys = column_idx.keySet();
        List<String> ans = new ArrayList<String>();
        for( int i:keys )  ans.add(column_idx.get(i));
        return ans;
    }
    public void addRow(List<String> str){
        if (column.size()!=str.size()) return;
        rows.add(str);
    }
    public void merge(Table tb, String[] on){
        for(int k = 0;k<on.length;k++){
            if( column.containsKey(on[k]) == false ) return;
            if( tb.column.containsKey(on[k]) == false ) return;
        }
        Map<String,Integer> ans_column = new HashMap<String,Integer>();
        Map<Integer,String> ans_column_idx = new HashMap<Integer,String>();
        int idx = 0;
        for(int i =0;i<column.size();i++){
            ans_column.put( tableName + "." + column_idx.get(i),idx);
            ans_column_idx.put(idx++,tableName + "." + column_idx.get(i));
        }
        for(int i =0;i<tb.column.size();i++){
            ans_column.put( tb.tableName + "." + tb.column_idx.get(i),idx);
            ans_column_idx.put(idx++,tb.tableName + "." + tb.column_idx.get(i));
        }
        Vector<List<String>> ans_rows = new Vector<List<String>>();
        // loop merge
        for(int i =0;i<rows.size();i++){
            for(int j =0;j<tb.rows.size();j++){
                boolean b = true;
                for(int k = 0;k<on.length;k++){
                    int row_idx = Integer.valueOf(column.get(on[k]));
                    int tb_row_idx = Integer.valueOf(tb.column.get(on[k]));
                    b = b & rows.get(i).get(row_idx).equals(tb.rows.get(j).get(tb_row_idx));
                }
                if( b == true ) {
                    List<String> tmp = new ArrayList<String>();
                    for(int k=0;k<rows.get(i).size();k++)  tmp.add( rows.get(i).get(k) );
                    for(int k=0;k<tb.rows.get(j).size();k++)  tmp.add( tb.rows.get(j).get(k) );
                    ans_rows.add(tmp);
                }
            }
        }
        column = ans_column;
        column_idx = ans_column_idx;
        rows = ans_rows;
    }
    public void slice(String[] on){
        Map<String,Integer> ans_column = new HashMap<String,Integer>();
        Map<Integer,String> ans_column_idx = new HashMap<Integer,String>();
        int idx = 0;
        for(int k = 0;k<on.length;k++){
            if( column.containsKey(on[k]) == false ) return;
            ans_column.put( on[k],idx);
            ans_column_idx.put(idx++,on[k]);
        }
        Vector<List<String>> ans_rows = new Vector<List<String>>();
        for(int i =0;i<rows.size();i++){
            List<String> tmp = new ArrayList<String>();
            for(int j =0;j<on.length;j++){
                tmp.add( rows.get(i).get(column.get(on[j])) );
            }
            ans_rows.add(tmp);
        }
        column = ans_column;
        column_idx = ans_column_idx;
        rows = ans_rows;
    }
    public void watchTable(){
        List<String> co = getColumn();
        for(int i =0;i<co.size();i++){
            System.out.print(co.get(i)+" ");
        }
        System.out.println();
        for(int i =0;i<rows.size();i++){
            for(int j =0;j<rows.get(i).size();j++) {
                System.out.print(rows.get(i).get(j)+" ");
            }
            System.out.println();
        }
    }
    public static void main(String args[]){
        Table t1 = new Table("user");
        List<String> c1 = new ArrayList<String>(); c1.add("userID"); c1.add("age");
        t1.setColumn(c1);
        List<String> row1 = new ArrayList<String>(); row1.add("001"); row1.add("14"); t1.addRow(row1);
        List<String> row11 = new ArrayList<String>(); row11.add("001"); row11.add("28"); t1.addRow(row11);
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
        on[0]="user.age";
        t1.slice(on);
        t1.watchTable();
    }
}