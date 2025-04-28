package preprocess;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class TagsFlattenDriver {
    public static void main(String[] args) throws IOException, URISyntaxException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TagsFlatten");

        job.addCacheFile(new URI("hdfs://pasak8s-20:9001/input/book_tags.csv#bookTags"));
        job.setNumReduceTasks(2);

        job.setMapperClass(TagsFlattenMapper.class);
        job.setReducerClass(TagsFlattenReducer.class);
        job.setJarByClass(TagsFlattenDriver.class);
    }
}
