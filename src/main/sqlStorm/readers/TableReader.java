package readers;

import java.io.*;

public class TableReader implements Serializable {
    public String fileName="./data/user.csv";
    public int column_num = 0;
    public int index_num = 0;
    public String [] columns = new String[10];
    public String [][] data = new String[100][10];
    public TableReader(){}
    public TableReader(String name){
        fileName = name;
        read(fileName);
    }
    public void init(){
        column_num = 0;
        index_num = 0;
        columns = new String[10];
        data = new String[100][10];
    }
    public static void main(String args[]){
        //read("./data/user.csv");
        //test();
    }
    public void read(String fileName){
        File file = new File(fileName);
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            columns = reader.readLine().split(",");
            column_num = columns.length;
            index_num = 0;
            while ((tempString = reader.readLine()) != null) {
                data[index_num] = tempString.split(",");
                index_num += 1 ;
            }
            reader.close();
        } catch (IOException e) { e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
    }
    public String[] getColumns() { return columns; }
    public String[][] getData(){ return data; }
    public void test(){
        String [] tmp = getColumns();
        for (int i =0;i<column_num;i++ ){
            System.out.print(tmp[i] + " ");
        }
        System.out.println();
        String [][] tmp2 = getData();
        for (int i = 0;i<index_num;i++){
            for (int j =0;j<column_num;j++) {
                System.out.print(data[i][j]+ " ");
            }
            System.out.println();
        }
    }
}
