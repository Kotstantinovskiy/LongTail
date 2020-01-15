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

public class GetICFbigramm extends Configured implements Tool {

    public static class ICFMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        HashSet<String> query_words_bigram = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException {
            Path queriesFile = new Path(Config.QUERIES);
            FileSystem fs = queriesFile.getFileSystem(context.getConfiguration());
            FSDataInputStream file = fs.open(queriesFile);

            BufferedReader reader = new BufferedReader(new InputStreamReader(file, StandardCharsets.UTF_8));
            String query_line = reader.readLine();
            while (query_line != null && !query_line.equals("")) {
                if (query_line.split("\t").length == 2) {
                    String query = query_line.split("\t")[1];
                    String[] query_words_split = query.split(" ");

                    if (query_words_split.length > 1){
                        for (int i=1; i < query_words_split.length; i++){
                            if(query_words_split[i-1].length() > 0 && query_words_split[i].length() > 0) {
                                query_words_bigram.add(query_words_split[i-1] + " " + query_words_split[i]);
                            }
                        }
                    }
                }

                query_line = reader.readLine();
            }
            reader.close();
        }

        @Override
        public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
            String[] parts = line.toString().split("\t");

            if(parts.length == 2) {
                String title = parts[1];
                String[] title_split = title.split(" ");
                HashSet<String> titleTokensSet = new HashSet<>();

                if (title_split.length > 1){
                    for (int i=1; i < title_split.length; i++){
                        if(title_split[i-1].length() > 0 && title_split[i].length() > 0) {
                            titleTokensSet.add(title_split[i - 1] + " " + title_split[i]);
                        }
                    }
                }

                HashMap<String, Integer> freqTokensMap = new HashMap<>();
                for (String word: titleTokensSet) {
                    int freq = freqTokensMap.getOrDefault(word, 0);
                    freqTokensMap.put(word, freq+1);
                }

                for(String word : titleTokensSet) {
                    if(query_words_bigram.contains(word)) {
                        context.write(new Text("ALL " + word), new LongWritable(freqTokensMap.get(word)));
                        context.write(new Text("TITLE " + word), new LongWritable(freqTokensMap.get(word)));
                    }
                }
            } else if(parts.length == 3) {
                String title = parts[1];
                String text = parts[2];
                String[] title_split = title.split(" ");
                String[] text_split = text.split(" ");
                HashSet<String> titleTokensSet = new HashSet<>();
                HashSet<String> textTokensSet = new HashSet<>();

                if (title_split.length > 1){
                    for (int i=1; i < title_split.length; i++){
                        if(title_split[i-1].length() > 0 && title_split[i].length() > 0) {
                            titleTokensSet.add(title_split[i-1] + " " + title_split[i]);
                        }
                    }
                }

                HashMap<String, Integer> freqTitleMap = new HashMap<>();
                for (String word: titleTokensSet) {
                    int freq = freqTitleMap.getOrDefault(word, 0);
                    freqTitleMap.put(word, freq+1);
                }

                if (text_split.length > 1){
                    for (int i=1; i < text_split.length; i++){
                        if(text_split[i-1].length() > 0 && text_split[i].length() > 0) {
                            textTokensSet.add(text_split[i-1] + " " + text_split[i]);
                        }
                    }
                }

                HashMap<String, Integer> freqTextMap = new HashMap<>();
                for (String word: textTokensSet) {
                    int freq = freqTextMap.getOrDefault(word, 0);
                    freqTextMap.put(word, freq+1);
                }

                for(String word : titleTokensSet) {
                    if(query_words_bigram.contains(word)) {
                        context.write(new Text("ALL " + word), new LongWritable(freqTitleMap.get(word)));
                        context.write(new Text("TITLE " + word), new LongWritable(freqTitleMap.get(word)));
                    }
                }

                for(String word : textTokensSet) {
                    if(query_words_bigram.contains(word)) {
                        context.write(new Text("ALL " + word), new LongWritable(freqTextMap.get(word)));
                        context.write(new Text("TEXT " + word), new LongWritable(freqTextMap.get(word)));
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
                n = n + 1;
            }

            context.write(key, new LongWritable(n));
        }
    }

    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new GetICFbigramm(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Two parameters are required :)");
            return -1;
        }

        Job job = Job.getInstance(getConf());
        job.setJobName("CALCULATING_BIGRAMM_ICF");

        FileSystem fs = FileSystem.get(getConf());
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }

        job.setJarByClass(GetICFbigramm.class);
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
