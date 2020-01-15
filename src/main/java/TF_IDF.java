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

public class TF_IDF extends Configured implements Tool {

    public static class TFIDFMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        Map<String, Double> df = new HashMap<>();
        Map<Integer, String> queries = new HashMap<>();
        Map<Integer, ArrayList<Integer>> docQueries =  new HashMap<>();

        @Override
        protected void setup(Mapper.Context context) throws IOException {
            Path docs_df = new Path(Config.DOCS_ALL_IDF);
            FileSystem fs = docs_df.getFileSystem(context.getConfiguration());
            FSDataInputStream file = fs.open(docs_df);
            BufferedReader reader = new BufferedReader(new InputStreamReader(file, StandardCharsets.UTF_8));
            String word_df = reader.readLine();
            while (word_df != null && !word_df.equals("")) {
                df.put(word_df.split("\t")[0], Double.valueOf(word_df.split("\t")[1]));
                word_df = reader.readLine();
            }
            reader.close();

            Path queriesFile = new Path(Config.QUERIES);
            fs = queriesFile.getFileSystem(context.getConfiguration());
            file = fs.open(queriesFile);

            reader = new BufferedReader(new InputStreamReader(file, StandardCharsets.UTF_8));
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

            if (parts.length == 3) {
                String doc_id = parts[0];
                String title = parts[1];
                String text = parts[2];

                ArrayList<Integer> idxQueries = docQueries.get(Integer.valueOf(doc_id));
                if (idxQueries != null) {
                    ArrayList<String> textTokens = new ArrayList<>(Arrays.asList(text.split(" ")));
                    ArrayList<String> titleTokens = new ArrayList<>(Arrays.asList(title.split(" ")));

                    double lenText = (double) textTokens.size();
                    double lenTitle = (double) titleTokens.size();

                    HashMap<String, Double> freqTextMap = new HashMap<>();
                    for (String word : textTokens) {
                        double freq = freqTextMap.getOrDefault(word, 0.0);
                        freqTextMap.put(word, freq + 1);
                    }

                    for (String w : freqTextMap.keySet()) {
                        if (w.length() != 0) {
                            if (df.containsKey(w)) {
                                for (int idx : idxQueries) {
                                    String query = queries.get(idx);
                                    if (query != null) {
                                        if (query.length() != 0) {
                                            for (String ww : query.split(" ")) {
                                                if (w.equals(ww)) {
                                                    double idf = Math.log(Config.N / df.get(w));
                                                    if (idf > 0) {
                                                        context.write(new Text("TEXT " + doc_id + " " + String.valueOf(idx)),
                                                                new DoubleWritable((freqTextMap.get(w) / lenText) * idf));
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    HashMap<String, Double> freqTitleMap = new HashMap<>();
                    for (String word : titleTokens) {
                        double freq = freqTitleMap.getOrDefault(word, 0.0);
                        freqTitleMap.put(word, freq + 1);
                    }

                    for (String w : freqTitleMap.keySet()) {
                        if (w.length() != 0) {
                            if (df.containsKey(w)) {
                                for (int idx : idxQueries) {
                                    String query = queries.get(idx);
                                    if (query != null) {
                                        if (query.length() != 0) {
                                            for (String ww : query.split(" ")) {
                                                if (w.equals(ww)) {
                                                    double idf = Math.log(Config.N / df.get(w));
                                                    if (idf > 0) {
                                                        context.write(new Text("TITLE " + doc_id + " " + String.valueOf(idx)),
                                                                new DoubleWritable((freqTitleMap.get(w) / lenTitle) * idf));
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            } else if(parts.length == 2) {
                String doc_id = parts[0];
                String title = parts[1];

                ArrayList<Integer> idxQueries = docQueries.get(Integer.valueOf(doc_id));
                if (idxQueries != null) {
                    ArrayList<String> titleTokens = new ArrayList<>(Arrays.asList(title.split(" ")));
                    double lenTitle = (double) titleTokens.size();

                    HashMap<String, Double> freqTitleMap = new HashMap<>();
                    for (String word : titleTokens) {
                        double freq = freqTitleMap.getOrDefault(word, 0.0);
                        freqTitleMap.put(word, freq + 1);
                    }

                    for (String w : freqTitleMap.keySet()) {
                        if (w.length() != 0) {
                            if (df.containsKey(w)) {
                                for (int idx : idxQueries) {
                                    String query = queries.get(idx);
                                    if (query != null) {
                                        if (query.length() != 0) {
                                            for (String ww : query.split(" ")) {
                                                if (w.equals(ww)) {
                                                    double idf = Math.log(Config.N / df.get(w));
                                                    if (idf > 0) {
                                                        context.write(new Text("TITLE " + doc_id + " " + String.valueOf(idx)),
                                                                new DoubleWritable((freqTitleMap.get(w) / lenTitle) * idf));
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    public static class TFIDFReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
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
        int exitCode = ToolRunner.run(new TF_IDF(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Three parameters are required :)");
            return -1;
        }

        FileSystem fs = FileSystem.get(getConf());
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }
        Job job = Job.getInstance(getConf());
        job.setJobName("FIRST_STAGE");

        job.setJarByClass(TF_IDF.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(TFIDFMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setReducerClass(TFIDFReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setNumReduceTasks(Config.REDUCE_COUNT);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }
}