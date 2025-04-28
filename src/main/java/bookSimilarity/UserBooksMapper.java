package bookSimilarity;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class UserBooksMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        /*
        读取 ratings.csv 文件
        输入格式：
        K：偏移量
        V：user_id, book_id, rating
         */
        // 跳过 csv 标题行
        if (value.toString().startsWith("user_id"))
            return;

        String line = value.toString();
        String[] fields = line.split(",");
        String user_id = fields[0].trim();
        String book_id = fields[1].trim();

        /*
        输出格式：
        K：user_id
        V：book_id
         */
        context.write( new Text(user_id), new Text(book_id));
    }
}
