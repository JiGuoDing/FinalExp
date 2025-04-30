package bookSimilarity;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URISyntaxException;

public class BooksSimilarityDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        Configuration conf = new Configuration();
        /*
        设置缓存值
         */
        conf.setInt("MAX_YEAR_GAP", 3760);
        conf.setInt("USER_NUM", 949895);
        Job job = Job.getInstance(conf, "BooksSimilarityDriver");
        /*
        添加图书信息文件到缓存中
         */
        job.addCacheFile(new Path("hdfs:///simplified_books/part-r-00000").toUri());
        job.addCacheFile(new Path("hdfs:///simplified_books/part-r-00001").toUri());
        /*
        添加图书标签信息文件到缓存中
         */
        job.addCacheFile(new Path("hdfs:///book_tags_flattened/part-r-00000").toUri());
        job.addCacheFile(new Path("hdfs:///book_tags_flattened/part-r-00001").toUri());
        job.setNumReduceTasks(3);

        job.setJarByClass(BooksSimilarityDriver.class);
        job.setMapperClass(BooksSimilarityMapper.class);
        job.setReducerClass(BooksSimilarityReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
