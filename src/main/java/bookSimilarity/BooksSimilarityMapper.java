package bookSimilarity;

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
import utils.TagCnt;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

public class BooksSimilarityMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final Logger logger = LoggerFactory.getLogger(BooksSimilarityMapper.class);
    /*
    保存结构：book_id: Book(book_id, goodreads_book_id, best_book_id,...)
     */
    Map<String, Book> booksMap = new HashMap<>();
    /*
    保存结构：book_id: List[(tag, cnt)...]
     */
    Map<String, List<TagCnt>> tagsMap = new HashMap<>();

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        /*
        读取缓存中的图书信息文件和图书标签信息文件
         */
        CSVParser csvParser = new CSVParserBuilder().withSeparator(',').withQuoteChar('"').build();
        URI[] cacheFiles = context.getCacheFiles();
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);

        for (URI cacheFile : cacheFiles) {
            String filePath = cacheFile.getPath();

                try(FSDataInputStream inputStream = fs.open(new Path(cacheFile));
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream))) {
                    String line;
                    if (filePath.contains("simplified_books")){
                        /*
                        读取 books_simplified 文件（格式：book_id, goodreads_book_id, best_book_id, work_id, authors, original_publication_decade, title）
                         */
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
                            booksMap.putIfAbsent(book_id, new Book(book_id, goodreads_book_id, best_book_id, work_id, authors, original_publication_decade, title));
                        }
                    } else {
                        /*
                        读取 book_tags_flattened 文件（格式：book_id    tag_id1:cnt1, tag_id2:cnt2, tag_id3:cnt3,...）
                         */
                        while ((line = bufferedReader.readLine()) != null){
                            String[] fields = line.split("\t");
                            String book_id = fields[0];
                            String tags_cnts = fields[1];
                            List<TagCnt> list_tags_cnts = Arrays.stream(tags_cnts.split(",")).map(String::trim).map(tag_cnt -> {
                                String[] split = tag_cnt.split(":");
                                String tag_id = split[0];
                                int cnt =  Integer.parseInt(split[1]);
                                return new TagCnt(tag_id, cnt);
                            }).collect(Collectors.toList());

                            tagsMap.putIfAbsent(book_id, list_tags_cnts);
                        }
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
        输入结构：
        K：book_id1, book_id2
        V：cooccurrence
         */
        String[] book_ids = key.toString().split(",");
        String book_id1 = book_ids[0].trim();
        String book_id2 = book_ids[1].trim();
        int cooccurrence = Integer.parseInt(value.toString());

        /*
        获取两本图书的信息
         */
        Book book1 = booksMap.get(book_id1);
        Book book2 = booksMap.get(book_id2);

        /*
        获取两本图书的标签信息
         */
        List<TagCnt> list_tagCnt1 = tagsMap.get(book_id1);
        List<TagCnt> list_tagCnt2 = tagsMap.get(book_id2);

        /*
        TODO 获取两本图书的作者信息
         */

        /*
        TODO 获取两本图书的年代信息
         */

        /*
        TODO 计算两本图书的相似度
         */

    }
}
