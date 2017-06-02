package readers;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.io.FileNotFoundException;
import java.util.regex.*;
public class SQLReader {
    public static String fileName="./data/sql.txt";
    public static String SQLmodel = "null";
    public static String []select;
    public static String []from;
    public static String []where;
    public static String []groupby;
    public SQLReader(){}
    public static void read(){
        File file = new File(fileName);
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            SQLmodel = reader.readLine().replace("#","");
            String sql = reader.readLine();
            reader.close();
            ///////////////////  更新select ///////////////////
            Pattern p = Pattern.compile("SELECT[ (),.a-zA-Z0-9_]+FROM");
            Matcher m = p.matcher(sql);
            if (m.find()){
                String tmp = m.group(0).replace("SELECT","").replace("FROM","");
                select = tmp.replace(" ","").split(",");
            }else{ select = new String[0]; }
            ///////////////////// 更新from ///////////////////
            p = Pattern.compile("(FROM[ ,.a-zA-Z0-9_]+WHERE)|(FROM[ ,.a-zA-Z0-9_]+GROUP BY)|(FROM[ ,.a-zA-Z0-9_]+)");
            m = p.matcher(sql);
            if (m.find()){
                String tmp = m.group(0).replace("FROM","").replace("WHERE","");
                tmp = tmp.replace("GROUP BY","");
                from = tmp.replace(" ","").split(",");
            }else{ from = new String[0]; }
            ///////////////////// 更新where ///////////////////
            p = Pattern.compile("(WHERE[ ,.a-zA-Z0-9_\"=]+GROUP BY)|(WHERE[ ,.a-zA-Z0-9_\"=]+)");
            m = p.matcher(sql);
            if (m.find()){
                String tmp = m.group(0).replace("WHERE","").replace("GROUP BY","");
                where = tmp.replace(" ","").split("and");
            }else{ where = new String[0]; }
            ///////////////////// 更新groupby ///////////////////
            p = Pattern.compile("GROUP BY[ ,.a-zA-Z0-9_]+");
            m = p.matcher(sql);
            if (m.find()){
                String tmp = m.group(0).replace("GROUP BY","");
                groupby = tmp.replace(" ","").split(",");
            }else{ groupby = new String[0]; }
        } catch (IOException e) { e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
        //test();
    }
    public static void main(String args[]){
        read();
        test();
    }
    public static void test(){
        System.out.println(SQLmodel);
        System.out.print("select:"); for (String s:select) System.out.print(s+" "); System.out.println();
        System.out.print("from:"); for (String s:from) System.out.print(s+" "); System.out.println();
        System.out.print("where:"); for (String s:where) System.out.print(s+" "); System.out.println();
        System.out.print("groupby:"); for (String s:groupby) System.out.print(s+" "); System.out.println();
    }
}
