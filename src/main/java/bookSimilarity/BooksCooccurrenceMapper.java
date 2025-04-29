package bookSimilarity;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BooksCooccurrenceMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        /*
        输入格式：
        K：user_id
        V：book_id1, book_id2, book_id3,...
         */
        String line = value.toString();
        String[] split = line.split("\t");
        String user_id =  split[0].trim();
        String book_ids = split[1].trim();
        String[] ary_book_ids = book_ids.split(",");
        for (int i = 0; i < ary_book_ids.length; i++) {
            for (int j = i + 1; j < ary_book_ids.length; j++) {
                /*
                输出格式：
                K：book_id_i, book_id_j
                V：1
                 */
                String composite_book_ids = ary_book_ids[i] + "," + ary_book_ids[j];
                context.write(new Text(composite_book_ids), new IntWritable(1));
            }
        }
    }
}
