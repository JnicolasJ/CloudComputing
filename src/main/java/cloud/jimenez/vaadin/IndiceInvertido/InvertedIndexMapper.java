package cloud.jimenez.vaadin.IndiceInvertido;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;


public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
    List<String> ignoreWords = new ArrayList<String>();
    String key_search;
    //leer el ignoreWord.txt desde filesystem
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        if(conf.get("filePath") != null){
            Path pt = new Path(conf.get("filePath")); //locacion de ignoreWords
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line;
            line = br.readLine();
            while (line != null) {
                ignoreWords.add(line.trim().toLowerCase());
                line = br.readLine();
            }
        }
        key_search = conf.get("key_search");
    }

    //btener cada palabra del archivo, agregar al contexto si no a ignoreWords
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        Text name = new Text(fileName);

        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        while(tokenizer.hasMoreTokens()){
            String curWord = tokenizer.nextToken().toString().toLowerCase();
            curWord = curWord.replaceAll("[^a-zA-Z]", ""); //deshacerse de los caracteres que no son alfabetos
            if(!ignoreWords.contains(curWord)){
                context.write(new Text(curWord), name);
            }
        }


/*
        StringTokenizer tokenizer = new StringTokenizer(key_search);
        while(tokenizer.hasMoreTokens()){
            String curWord = tokenizer.nextToken().toString().toLowerCase();
            curWord = curWord.replaceAll("[^a-zA-Z]", ""); //deshacerse de los caracteres que no son alfabetos
            if(!ignoreWords.contains(curWord)){
                context.write(new Text(curWord), name);
            }
        }
*/
    }

}