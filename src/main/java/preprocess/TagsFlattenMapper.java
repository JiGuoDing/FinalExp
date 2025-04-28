package preprocess;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

public class TagsFlattenMapper extends Mapper<Text, Text, Text, Text> {

    @Override
    protected void setup(Mapper<Text, Text, Text, Text>.Context context) throws IOException {
        // 读取 book_tags.csv 文件（格式：goodreads_book_id, tag_id, count）
        URI[] files = context.getCacheFiles();
        BufferedReader bufferedReader = new BufferedReader(new FileReader("bookTags"));
        String line;
        while ((line = bufferedReader.readLine()) != null){
            String[] fields = line.split(",");
            String goodreads_book_id = fields[0];
            String tag_id =  fields[1];
            String count = fields[2];
        }
    }

    @Override
    protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context) {

    }
}
