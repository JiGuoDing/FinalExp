package bookSimilarity;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UserBooksReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        /*
        输入格式：
        K：user_id
        V：book_id
         */
        List<String> list_book_id = new ArrayList<>();
        for (Text value : values) {
            list_book_id.add(value.toString());
        }
        /*
        将图书序号按升序排列
         */

        String book_ids =  String.join(",", list_book_id);

        /*
        输出格式：
        K：user_id
        V：book_id1, book_id2, book_id3,...
         */
        context.write(key, new Text(book_ids));
    }
}
