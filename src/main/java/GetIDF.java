import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
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
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class GetIDF extends Configured implements Tool {

    public static class IDFMapper extends Mapper<LongWritable, Text, Text, Text> {

        HashSet<String> query_words = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException {
            Path queriesFile = new Path(Config.QUERIES_SET);
            FileSystem fs = queriesFile.getFileSystem(context.getConfiguration());
            FSDataInputStream file = fs.open(queriesFile);

            BufferedReader reader = new BufferedReader(new InputStreamReader(file, StandardCharsets.UTF_8));
            String w = reader.readLine();

            while (w != null && !w.equals("")) {
                query_words.add(w);
                w = reader.readLine();
            }
            //query_words_array.addAll(Arrays.asList(query.split("\t")[1].split(" ")));
            //query_words = query_words_array.stream().distinct().collect(Collectors.toList());
            reader.close();
        }

        @Override
        public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
            String[] parts = line.toString().split("\t");

            if(parts.length == 2) {
                String title = parts[1];
                HashSet<String> titleTokensSet = new HashSet<>(Arrays.asList(title.split(" ")));

                for(String word : titleTokensSet) {
                    if(word.length() > 0) {
                        if(query_words.contains(word)) {
                            //System.out.println(word);
                            context.write(new Text("TITLE " + word), new Text("1"));
                            context.write(new Text("ALL " + word), new Text("1"));
                        }
                    }
                }
            } else if(parts.length == 3) {
                /*
                System.out.println("11111111111111111111111111111111111111111111111");
                for(String w : query_words.subList(0, 30)) {
                    System.out.println(w);
                }
                System.out.println("22222222222222222222222222222222222222222222222");
                */
                String title = parts[1];
                String text = parts[2];
                HashSet<String> titleTokensSet = new HashSet<>(Arrays.asList(title.split(" ")));
                HashSet<String> textTokensSet = new HashSet<>(Arrays.asList(text.split(" ")));

                for(String word : titleTokensSet) {
                    //System.out.println(word);
                    if(word.length() > 0) {
                        if(query_words.contains(word)) {
                            System.out.println(word);
                            context.write(new Text("TITLE " + word), new Text("1"));
                            context.write(new Text("ALL " + word), new Text("1"));
                        }
                    }
                }

                for(String word : textTokensSet) {
                    if(word.length() > 0) {
                        if(query_words.contains(word)) {
                            context.write(new Text("TEXT " + word), new Text("1"));
                            context.write(new Text("ALL " + word), new Text("1"));
                        }
                    }
                }
            }
        }
    }

    public static class IDFReducer extends Reducer<Text, Text, Text, LongWritable> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long n = 0;

            for(Text val: values){
                n = n + 1;
            }

            context.write(key, new LongWritable(n));
        }
    }

    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new GetIDF(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Two parameters are required :)");
            return -1;
        }

        Job job = Job.getInstance(getConf());
        job.setJobName("CALCULATING_IDF");

        FileSystem fs = FileSystem.get(getConf());
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }

        job.setJarByClass(GetIDF.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(IDFMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(IDFReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setNumReduceTasks(Config.REDUCE_COUNT);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }
}
