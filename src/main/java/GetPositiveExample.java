import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;

public class GetPositiveExample extends Configured implements Tool {

    public static class AllMapper extends Mapper<LongWritable, Text, Text, Text> {

        HashMap<String, Integer> urlId = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, NullPointerException{
            Path idQueryPath = new Path(Config.PATH_ID_QUERY);
            Path idUrlPath = new Path(Config.PATH_ID_URL);
            String str;

            FileSystem fs = idQueryPath.getFileSystem(context.getConfiguration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(idUrlPath), StandardCharsets.UTF_8));
            while ((str = br.readLine()) != null) {
                String[] id_url = str.split("\t");
                id_url[1] = id_url[1].trim()
                        .replace("http://", "")
                        .replace("https://", "")
                        .replace("www.", "");
                urlId.put(id_url[1], Integer.valueOf(id_url[0]));
            }
            br.close();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            SERP serp = new SERP(value.toString());

            for(String clickUrl: serp.clicked){
                if(urlId.get(clickUrl) != null) {
                    context.write(new Text(String.valueOf(serp.query)), new Text(String.valueOf(urlId.get(clickUrl))));
                }
            }
        }
    }

    public static class AllReducer extends Reducer<Text, Text, Text, Text> {
        HashMap<String, Integer> urlId = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, NullPointerException{
            Path idQueryPath = new Path(Config.PATH_ID_QUERY);
            Path idUrlPath = new Path(Config.PATH_ID_URL);
            String str;

            FileSystem fs = idQueryPath.getFileSystem(context.getConfiguration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(idUrlPath), StandardCharsets.UTF_8));
            while ((str = br.readLine()) != null) {
                String[] id_url = str.split("\t");
                id_url[1] = id_url[1].trim()
                        .replace("http://", "")
                        .replace("https://", "")
                        .replace("www.", "");
                urlId.put(id_url[1], Integer.valueOf(id_url[0]));
            }
            br.close();
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashSet<String> set = new HashSet<>();
            for(Text val: values) {
                set.add(key.toString() + "\t" + val.toString());
            }

            for(String s: set){
                context.write(new Text(s.split("\t")[0]), new Text(s.split("\t")[1]));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new GetPositiveExample(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.out.println("Two parameters are required!");
            return -1;
        }

        Job job = Job.getInstance(getConf());
        job.setJobName("GetPositiveExample");

        FileSystem fs = FileSystem.get(getConf());
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }

        job.setJarByClass(ExctractFeatures.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(AllMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(AllReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(Config.REDUCE_COUNT);

        boolean success = job.waitForCompletion(true);

        return success ? 0 : 1;
    }
}


