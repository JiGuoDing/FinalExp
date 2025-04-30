package preprocess;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.DecadeConverter;

import java.io.IOException;
import java.util.StringJoiner;

/**
 * 数据预处理
 */
public class PreprocessMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private static final Logger logger = LoggerFactory.getLogger(PreprocessMapper.class);
    private CSVParser csvParser;

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context) {
        csvParser = new CSVParserBuilder().withSeparator(',').withQuoteChar('"').build();
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        logger.info("PreprocessMapper start");
        /*
        输入：图书表的一行记录
         */
        // 跳过 csv 标题行
        if (value.toString().startsWith("book_id"))
            return;

        String line = value.toString();
        // 解析出每一个字段
        String[] fields = csvParser.parseLine(line);
        String book_id = fields[0];
        String goodreads_book_id = fields[1];
        String best_book_id = fields[2];
        String work_id = fields[3];
        String authors = "\"" + fields[7] + "\"";
        String original_publication_year = fields[8];
        String decade;
        // 如果年份为空，则将其指定为 1900s
        if (original_publication_year.isEmpty())
            // 将具体年份转换为十年制的年代
            decade = "1900s";
        else decade = DecadeConverter.toDecade(original_publication_year);
        String title = "\"" + fields[10] + "\"";

        StringJoiner joiner = new StringJoiner(",");
        joiner.add(book_id);
        joiner.add(goodreads_book_id);
        joiner.add(best_book_id);
        joiner.add(work_id);
        joiner.add(authors);
        joiner.add(decade);
        joiner.add(title);

        String simplified_fields = joiner.toString();
        // NullWritable 被设计为单例模式，其构造器是私有的，通过静态方法 get() 返回唯一的共享实例
        context.write(new Text(simplified_fields), NullWritable.get());
    }

    public static void main(String[] args) throws IOException {
        CSVParser csvParser = new CSVParserBuilder().withSeparator(',').withQuoteChar('"').build();
        String line = "16,2429135,2429135,1708725,274,307269752,9.78030726975e+12,\"Stieg Larsson, Reg Keeland\",1986,Män som hatar kvinnor,\"The Girl with the Dragon Tattoo (Millennium, #1)\",eng,4.11,1808403,1929834,62543,54835,86051,285413,667485,836050,https://images.gr-assets.com/books/1327868566m/2429135.jpg,https://images.gr-assets.com/books/1327868566s/2429135.jpg";

        String[] fields = csvParser.parseLine(line);
        String book_id = fields[0];
        String goodreads_book_id = fields[1];
        String best_book_id = fields[2];
        String work_id = fields[3];
        String authors = "\"" + fields[7] + "\"";
        String original_publication_year = fields[8];
        // 将具体年转换为年代
        String decade = original_publication_year.replaceAll("(\\d{3})\\d.*", "$10s");
        logger.info(original_publication_year);
        String title = fields[10];

        StringJoiner joiner = new StringJoiner(",");
        joiner.add(book_id);
        joiner.add(goodreads_book_id);
        joiner.add(best_book_id);
        joiner.add(work_id);
        joiner.add(authors);
        joiner.add(decade);
        joiner.add(title);

        String simplified_fields = joiner.toString();
        logger.info(simplified_fields);
    }
}
