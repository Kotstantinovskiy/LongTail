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
import java.nio.charset.StandardCharsets;
import java.util.*;

public class GetICF extends Configured implements Tool {

    public static class ICFMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        HashSet<String> query_words = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException {
            Path queriesFile = new Path(Config.QUERIES);
            FileSystem fs = queriesFile.getFileSystem(context.getConfiguration());
            FSDataInputStream file = fs.open(queriesFile);

            BufferedReader reader = new BufferedReader(new InputStreamReader(file, StandardCharsets.UTF_8));
            String query = reader.readLine();
            while (query != null && !query.equals("")) {
                if (query.split("\t").length == 2) {
                    query_words.addAll(Arrays.asList(query.split("\t")[1].split(" ")));
                }
                query = reader.readLine();
            }

            reader.close();
        }

        @Override
        public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
            String[] parts = line.toString().split("\t");

            if(parts.length == 2) {
                String title = parts[1];
                ArrayList<String> titleTokensSet = new ArrayList<>(Arrays.asList(title.split(" ")));

                HashMap<String, Integer> freqTitleMap = new HashMap<>();
                for (String word: titleTokensSet) {
                    int freq = freqTitleMap.getOrDefault(word, 0);
                    freqTitleMap.put(word, freq+1);
                }

                for(String word : titleTokensSet) {
                    if(word.length() > 0) {
                        if(query_words.contains(word)) {
                            context.write(new Text("ALL " + word), new LongWritable(freqTitleMap.get(word)));
                            context.write(new Text("TITLE " + word), new LongWritable(freqTitleMap.get(word)));
                        }
                    }
                }
            } else if(parts.length == 3) {
                String title = parts[1];
                String text = parts[2];
                ArrayList<String> titleTokensSet = new ArrayList<>(Arrays.asList(title.split(" ")));
                ArrayList<String> textTokensSet = new ArrayList<>(Arrays.asList(text.split(" ")));

                HashMap<String, Integer> freqTextMap = new HashMap<>();
                for (String word: textTokensSet) {
                    int freq = freqTextMap.getOrDefault(word, 0);
                    freqTextMap.put(word, freq+1);
                }

                HashMap<String, Integer> freqTitleMap = new HashMap<>();
                for (String word: titleTokensSet) {
                    int freq = freqTitleMap.getOrDefault(word, 0);
                    freqTitleMap.put(word, freq+1);
                }

                for(String word : freqTextMap.keySet()) {
                    if(word.length() > 0) {
                        if(query_words.contains(word)) {
                            context.write(new Text("ALL " + word), new LongWritable(freqTextMap.get(word)));
                            context.write(new Text("TEXT " + word), new LongWritable(freqTextMap.get(word)));
                        }
                    }
                }

                for(String word : freqTitleMap.keySet()) {
                    if(word.length() > 0) {
                        if(query_words.contains(word)) {
                            context.write(new Text("ALL " + word), new LongWritable(freqTitleMap.get(word)));
                            context.write(new Text("TITLE " + word), new LongWritable(freqTitleMap.get(word)));
                        }
                    }
                }
            }
        }
    }

    public static class ICFReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long n = 0;
            for(LongWritable val: values){
                n = n + val.get();
            }

            context.write(key, new LongWritable(n));
        }
    }

    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new GetICF(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Two parameters are required :)");
            return -1;
        }

        Job job = Job.getInstance(getConf());
        job.setJobName("CALCULATING_ICF");

        FileSystem fs = FileSystem.get(getConf());
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }

        job.setJarByClass(GetICF.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(ICFMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setReducerClass(ICFReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setNumReduceTasks(Config.REDUCE_COUNT);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }
}
