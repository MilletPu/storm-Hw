package writers;

import java.io.FileWriter;
import java.io.IOException;

public class SELECTWriter {
    public SELECTWriter() {}
    public void write(String fileName, String content, boolean append) {
        try {
            FileWriter writer = new FileWriter(fileName, append);
            writer.write(content);
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
