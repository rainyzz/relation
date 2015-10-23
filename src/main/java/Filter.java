import com.google.common.base.Strings;
import com.google.common.collect.Sets;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Set;

/**
 * Created by rainystars on 10/23/2015.
 */
public class Filter {
    public static Set<String> dict = Sets.newHashSetWithExpectedSize(400 * 10000);
    public static final String BUILD_LIBRARY_PTAH = "C:\\Users\\rainystars\\Desktop\\relation\\final.txt";

    public static void buildDict(){
        long beginTime = System.currentTimeMillis();

        String fileName = BUILD_LIBRARY_PTAH;
        try {
            FileReader fr = new FileReader(new File(fileName));
            BufferedReader br = new BufferedReader(fr);
            String line;
            int count = 0;
            while ((line = br.readLine()) != null) {
                if(line.trim().length() < 2){
                    continue;
                }
                if(!Strings.isNullOrEmpty(line)){
                    dict.add(line.trim());
                }
                count++;
                if(count % 100000 == 0){
                    System.out.println(count);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        long endTime=System.currentTimeMillis();
        long costTime = (endTime - beginTime);
        System.out.println("Dict built in " + costTime / 1000.0 + "s");
    }
}
