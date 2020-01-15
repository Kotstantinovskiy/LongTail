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

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class TF_ICF extends Configured implements Tool {

    public static class TFICFMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
            String[] parts = line.toString().split("\t");

            if (parts.length == 3) {
                String doc_id = parts[0];
                String title = parts[1];
                String text = parts[2];

                ArrayList<String> textTokens = new ArrayList<>(Arrays.asList(text.split(" ")));
                ArrayList<String> titleTokens = new ArrayList<>(Arrays.asList(title.split(" ")));

                int lenText = textTokens.size();
                int lenTitle = titleTokens.size();

                HashMap<String, Integer> freqTextMap = new HashMap<>();
                for (String word: textTokens) {
                    int freq = freqTextMap.getOrDefault(word, 0);
                    freqTextMap.put(word, freq+1);
                }

                for (String w : freqTextMap.keySet()) {
                    if (w.length() != 0) {
                        context.write(new Text("TEXT " + doc_id + " " + String.valueOf(lenTitle) + " " + w), new LongWritable(freqTextMap.get(w)));
                    }
                }

                HashMap<String, Integer> freqTitleMap = new HashMap<>();
                for (String word: titleTokens) {
                    int freq = freqTitleMap.getOrDefault(word, 0);
                    freqTitleMap.put(word, freq+1);
                }

                for (String w : freqTitleMap.keySet()) {
                    if (w.length() != 0) {
                        context.write(new Text("TITLE " + doc_id + " " + String.valueOf(lenText) + " " + w), new LongWritable(freqTitleMap.get(w)));
                    }
                }
            } else if(parts.length == 2) {
                String doc_id = parts[0];
                String title = parts[1];

                ArrayList<String> titleTokens = new ArrayList<>(Arrays.asList(title.split(" ")));
                int lenTitle = titleTokens.size();

                HashMap<String, Integer> freqTitleMap = new HashMap<>();
                for (String word: titleTokens) {
                    int freq = freqTitleMap.getOrDefault(word, 0);
                    freqTitleMap.put(word, freq+1);
                }

                for (String w : freqTitleMap.keySet()) {
                    if (w.length() != 0) {
                        context.write(new Text("TITLE " + doc_id + " " + String.valueOf(lenTitle) + " " + w), new LongWritable(freqTitleMap.get(w)));
                    }
                }
            }
        }
    }

    public static class TFICFReducer extends Reducer<Text, LongWritable, Text, DoubleWritable> {

        Map<String, Integer> cf = new HashMap<>();

        @Override
        protected void setup(Reducer.Context context) throws IOException {
            Path docs_df = new Path(Config.DOCS_ALL_ICF);
            FileSystem fs = docs_df.getFileSystem(context.getConfiguration());
            FSDataInputStream file = fs.open(docs_df);
            BufferedReader reader = new BufferedReader(new InputStreamReader(file, StandardCharsets.UTF_8));
            String word_df = reader.readLine();
            while (word_df != null && !word_df.equals("")) {
                cf.put(word_df.split("\t")[0], Integer.valueOf(word_df.split("\t")[1]));
                word_df = reader.readLine();
            }
            reader.close();
        }

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            String marker = key.toString().split(" ")[0];
            String doc_id = key.toString().split(" ")[1];
            String word = key.toString().split(" ")[3];
            double len = Double.valueOf(key.toString().split(" ")[2]);

            switch (marker) {
                case "TEXT": {
                    double n = 0;
                    for (LongWritable val : values) {
                        n = n + 1;
                    }

                    //double tf = ((n/len) * (Config.k1 + 1)) / ((n/len) + Config.k1 * (1 - Config.b + Config.b*avgl));
                    double tf = n / len;
                    if(cf.containsKey(word)) {
                        double icf = Math.log(Config.LEN_ALL / cf.get(word));
                        if(icf > 0) {
                            context.write(new Text("TEXT " + doc_id + " " + word), new DoubleWritable(tf * icf));
                        }
                    }
                    break;
                }
                case "TITLE": {
                    double n = 0;
                    for (LongWritable val : values) {
                        n = n + 1;
                    }

                    //double tf = ((n/len) * (Config.k1 + 1)) / (n/len + Config.k1 * (1 - Config.b + Config.b*avgl));
                    double tf = n / len;
                    if(cf.containsKey(word)) {
                        double icf = Math.log(Config.LEN_ALL / cf.get(word));
                        if (icf > 0) {
                            context.write(new Text("TITLE " + doc_id + " " + word), new DoubleWritable(tf * icf));
                        }
                    }
                    break;
                }
            }
        }
    }

    public static class MergeMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        Map<Integer, String> queries = new HashMap<>();
        Map<Integer, ArrayList<Integer>> docQueries =  new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            Path queriesFile = new Path(Config.QUERIES);
            FileSystem fs = queriesFile.getFileSystem(context.getConfiguration());
            FSDataInputStream file = fs.open(queriesFile);

            BufferedReader reader = new BufferedReader(new InputStreamReader(file, StandardCharsets.UTF_8));
            String query = reader.readLine();
            while (query != null && !query.equals("")) {
                if(query.split("\t").length == 2) {
                    queries.put(Integer.valueOf(query.split("\t")[0]), query.split("\t")[1]);
                }
                query = reader.readLine();
            }
            reader.close();

            Path docQueriesFile = new Path(Config.DOC_QUERIES);
            file = fs.open(docQueriesFile);
            reader = new BufferedReader(new InputStreamReader(file, StandardCharsets.UTF_8));
            String doc_query = reader.readLine();
            while (doc_query != null && !doc_query.equals("")) {
                ArrayList<Integer> idxQuery = new ArrayList<>();
                for(String str: doc_query.split("\t")[1].split(" ")){
                    idxQuery.add(Integer.valueOf(str));
                }
                docQueries.put(Integer.valueOf(doc_query.split("\t")[0]), idxQuery);
                doc_query = reader.readLine();
            }
            reader.close();
        }

        @Override
        public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
            String[] parts = line.toString().split("\t");
            String marker = parts[0].split(" ")[0];
            int doc_id = Integer.valueOf(parts[0].split(" ")[1]);
            String word = parts[0].split(" ")[2];
            double tf_icf = Double.valueOf(parts[1]);
            ArrayList<Integer> idxQueries = docQueries.get(doc_id);

            if (idxQueries != null) {
                switch (marker) {
                    case "TEXT":
                        for (int idx : idxQueries) {
                            String query = queries.get(idx);
                            if (query != null) {
                                if (query.length() != 0) {
                                    for (String w : query.split(" ")) {
                                        if (w.equals(word)) {
                                            context.write(new Text("TEXT " + String.valueOf(doc_id) + " " + String.valueOf(idx)),
                                                    new DoubleWritable(tf_icf));
                                        }
                                    }
                                }
                            }
                        }
                        break;
                    case "TITLE":
                        for (int idx : idxQueries) {
                            String query = queries.get(idx);
                            if (query != null) {
                                if (query.length() != 0) {
                                    for (String w : query.split(" ")) {
                                        if (w.equals(word)) {
                                            context.write(new Text("TITLE " + String.valueOf(doc_id) + " " + String.valueOf(idx)),
                                                    new DoubleWritable(tf_icf));
                                        }
                                    }
                                }
                            }
                        }
                        break;
                }
            }
        }
    }

    public static class MergeReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double tf_idf = 0;

            for(DoubleWritable val: values){
                tf_idf = tf_idf + val.get();
            }

            context.write(new Text(key), new DoubleWritable(tf_idf));
        }
    }

    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new TF_ICF(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Two parameters are required :)");
            return -1;
        }

        FileSystem fs = FileSystem.get(getConf());
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }
        Job job = Job.getInstance(getConf());
        job.setJobName("FIRST_STAGE");

        job.setJarByClass(TF_ICF.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(TFICFMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setReducerClass(TFICFReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setNumReduceTasks(Config.REDUCE_COUNT);

        boolean success = job.waitForCompletion(true);
        if(!success){
            return 1;
        }

        job = Job.getInstance(getConf());
        job.setJobName("SECOND_STAGE");

        fs = FileSystem.get(getConf());
        if (fs.exists(new Path(args[1] + "_2"))) {
            fs.delete(new Path(args[1] + "_2"), true);
        }

        job.setJarByClass(TF_ICF.class);
        FileInputFormat.setInputPaths(job, new Path(args[1] + "/part-*"));
        FileOutputFormat.setOutputPath(job, new Path(args[1] + "_2"));

        job.setMapperClass(MergeMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setReducerClass(MergeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setNumReduceTasks(Config.REDUCE_COUNT);

        success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }
}
