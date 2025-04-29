package bookSimilarity;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class BooksCooccurrenceCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        /*
        输入格式：
        K：book_id_i, book_id_j
        V：list[1, 1, 1,...]
         */

        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }

        /*
        输出格式：
        K：book_id_i, book_id_j
        V：temporary cooccurrence count
         */
        context.write(key, new IntWritable(sum));
    }
}
