package readers;
import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
public class SQLReader implements Serializable {
    public String fileName="./data/sql.txt";
    public String []select;
    public String []from;
    public String []where;
    public String []groupby;
    public SQLReader(){}
    public void read(){
        File file = new File(fileName);
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
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
            p = Pattern.compile("(WHERE[ ,.a-zA-Z0-9_><!\"=]+GROUP BY)|(WHERE[ ,.a-zA-Z0-9_><!\"=]+)");
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
        //read();
        //test();
    }
    public void test(){
        System.out.print("select:"); for (String s:select) System.out.print(s+" "); System.out.println();
        System.out.print("from:"); for (String s:from) System.out.print(s+" "); System.out.println();
        System.out.print("where:"); for (String s:where) System.out.print(s+" "); System.out.println();
        System.out.print("groupby:"); for (String s:groupby) System.out.print(s+" "); System.out.println();
    }
}
