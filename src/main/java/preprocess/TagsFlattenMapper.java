package preprocess;


import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jline.utils.InputStreamReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.Book;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class TagsFlattenMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final Logger logger = LoggerFactory.getLogger(TagsFlattenMapper.class);
    // 记录格式：goodreads_book_id: (tag_id, count)
    Map<String, Book> booksMap = new HashMap<>();

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException {
        CSVParser csvParser = new CSVParserBuilder().withSeparator(',').withQuoteChar('"').build();
        // 读取 books_simplified 文件（格式：book_id, goodreads_book_id, best_book_id, work_id, authors, original_publication_decade, title）
        URI[] cacheFiles = context.getCacheFiles();
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);

        for (URI cacheFile : cacheFiles) {
            try(FSDataInputStream inputStream = fs.open(new Path(cacheFile));
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream))) {
                String line;
                while ((line = bufferedReader.readLine()) != null){
                    String[] fields = csvParser.parseLine(line);
                    String book_id = fields[0];
                    String goodreads_book_id = fields[1].trim();
                    String best_book_id = fields[2].trim();
                    String work_id = fields[3].trim();
                    String authors = fields[4].trim();
                    String original_publication_decade = fields[5].trim();
                    String title = fields[6].trim();

                    // 保存记录到哈希表
                    booksMap.putIfAbsent(goodreads_book_id, new Book(book_id, goodreads_book_id, best_book_id, work_id, authors, original_publication_decade, title));
                }
            }catch (Exception e){
                logger.error("Error reading book tags", e);
                e.printStackTrace();
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        /*
        输入：book_tags.csv 的一行记录（格式：goodreads_book_id, tag_id, count）
         */
        // 跳过 csv 标题行
        if (value.toString().startsWith("goodreads_book_id"))
            return;

        String line = value.toString();
        String[] fields = line.split(",");
        String goodreads_book_id = fields[0].trim();
        String tag_id = fields[1].trim();
        String count = fields[2].trim();

        Book book = booksMap.get(goodreads_book_id);
        String book_id = book.getBook_id();
        String tag_flattened = tag_id + ", " + count;

        /*
        输出格式：
        K：book_id
        V：tag_id, count
         */
        context.write(new Text(book_id), new Text(tag_flattened));
    }
}
