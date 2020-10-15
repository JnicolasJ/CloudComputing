package cloud.jimenez.vaadin.IndiceInvertido;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

//input: <keyword, <doc1, doc1, doc1, doc2, doc2, doc2, doc2>>
//output: <keyword, doc1\ndoc2>
public class invertedIndexReducer extends Reducer<Text,Text,Text,Text> {

    public void reduce(final Text key, final Iterable<Text> books, final Context context) throws IOException, InterruptedException {
        int minCount= 10; // para calificar para ser una palabra clave, debe suceder 100 veces o más en un libro
        StringBuilder sb = new StringBuilder();
        String prevBook = null;
        int count=0; //cantidad de veces que la palabra clave aparece en este libro
        //pasar por el valor de cada libro
        for (Text book: books){
            if(prevBook != null && book.toString().trim().equals(prevBook)){
                count++;
                continue;
            }
            //if curbook != prevbook, entonces el libro anterior ha terminado de contar, verifique si la palabra clave para el libro anterior> 100, si no, reinicie el contador
            if(prevBook != null && count < minCount){
                count=1;
                prevBook=book.toString().trim();
                continue;

            }
            if (prevBook == null){
                prevBook = book.toString().trim();
                count++;
                continue;
            }
            sb.append(prevBook);
            sb.append("\t");
            count=1;
            prevBook=book.toString().trim();
        }
        //agregando el último libro
        if (count >= minCount){
            sb.append(prevBook);
        }
        if(!sb.toString().trim().equals("")){
            context.write(key, new Text(sb.toString()));
        }

    }

}