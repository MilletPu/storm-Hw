package readers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.io.FileNotFoundException;

public class TableReader {
    //public static String fileName="./data/user.csv";
    public static int column_num = 0;
    public static int index_num = 0;
    public static String [] columns = new String[10];
    public static String [][] data = new String[100][10];
    public TableReader(){}
    public TableReader(String fileName){ read(fileName); }
    public static void main(String args[]){
        read("./data/user.csv");
        test();
    }
    public static void read(String fileName){
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
    public static String[] getColumns() { return columns; }
    public static String[][] getData(){ return data; }
    public static void test(){
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
