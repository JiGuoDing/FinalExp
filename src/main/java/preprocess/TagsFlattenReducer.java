package preprocess;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TagsFlattenReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        /*
        输入格式：
        K：book_id
        V：List(tag_id, count)
         */
        List<String> tagsCounts = new ArrayList<>();
        for (Text value : values) {
            String line = value.toString();
            String[] tagCount = line.split(",");
            String tag_id = tagCount[0];
            String count = tagCount[1];
            tagsCounts.add(tag_id + ":" + count);
        }
        /*
        输出格式：
        K：book_id
        V：tag_id:num,tag_id:num,...
         */
        String tags_flattened = String.join(",", tagsCounts);
        context.write(key, new Text(tags_flattened));
    }
}
