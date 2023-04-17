package model;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DataReader {
    public DataReader(){

    }
    public static List<String[]> readFile(String path, String split) {
        List<String[]> readData = new ArrayList<>();
        try{
            File file = new File(path);
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);

            String line = br.readLine();

            while(line != null && !line.isBlank()){
                String[] data = line.split(split);
                readData.add(data);
                line = br.readLine();
            }

            br.close();
        }catch (IOException e){
            e.printStackTrace();
        }

        return readData;
    }
}
