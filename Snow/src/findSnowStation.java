import java.io.*;

/**
 * Created by Jo on 01.04.14.
 */
public class findSnowStation {

    public static void main(String[] args)
    {
        String inputPath = args[0];
        String outputPath = args[1];

        File folder = new File(inputPath);

        File[] files = folder.listFiles();


        for(File f : files){
            FileInputStream fis = null;
            try {
                fis = new FileInputStream(f);
                InputStreamReader isr = new InputStreamReader(fis, "UTF8");

                String line = fis.


            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch(IOException io){
                io.printStackTrace();
            }




        }





    }

}
