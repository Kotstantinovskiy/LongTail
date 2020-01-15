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

public class AllWordsIDF extends Configured implements Tool {

    public static class AllWordsMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        Map<Integer, String> queries = new HashMap<>();
        Map<Integer, ArrayList<Integer>> docQueries = new HashMap<>();
        Map<String, Integer> df = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            //System.out.println("Start End mapper");
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
                ArrayList<Integer> idxDocs = new ArrayList<>();
                for(String str: doc_query.split("\t")[1].split(" ")){
                    idxDocs.add(Integer.valueOf(str));
                }

                docQueries.put(Integer.valueOf(doc_query.split("\t")[0]), idxDocs);
                doc_query = reader.readLine();
            }
            reader.close();

            Path docs_df = new Path(Config.DOCS_ALL_IDF);
            file = fs.open(docs_df);
            reader = new BufferedReader(new InputStreamReader(file, StandardCharsets.UTF_8));
            String word_df = reader.readLine();
            while (word_df != null && !word_df.equals("")) {
                df.put(word_df.split("\t")[0], Integer.valueOf(word_df.split("\t")[1]));
                word_df = reader.readLine();
            }
            reader.close();
        }

        @Override
        public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
            String[] parts = val.toString().split("\t");
            int doc_id = Integer.valueOf(parts[0].split(" ")[0]);
            ArrayList<Integer> idxQueries = docQueries.get(doc_id);

            if (parts.length == 3) {
                ArrayList<String> title = new ArrayList<>(Arrays.asList(parts[1].split(" ")));
                ArrayList<String> text = new ArrayList<>(Arrays.asList(parts[2].split(" ")));

                if (idxQueries != null) {
                    for(String word: title) {
                        for (int idx : idxQueries) {
                            String query = queries.get(idx);
                            if (query != null) {
                                for (String w_q : query.split(" ")) {
                                    if (w_q.equals(word)) {
                                        if (df.containsKey(word)) {
                                            if (Math.log(Config.N / df.get(word)) > 0) {
                                                context.write(new Text("TITLE " + String.valueOf(doc_id) + " " + String.valueOf(idx)),
                                                        new DoubleWritable(Math.log(Config.N / df.get(word))));
                                            } else {
                                                context.write(new Text("TITLE " + String.valueOf(doc_id) + " " + String.valueOf(idx)),
                                                        new DoubleWritable(0));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                if (idxQueries != null) {
                    for(String word: text) {
                        for (int idx : idxQueries) {
                            String query = queries.get(idx);
                            if (query != null) {
                                for (String w_q : query.split(" ")) {
                                    if (w_q.equals(word)) {
                                        if (df.containsKey(word)) {
                                            if (Math.log(Config.N / df.get(word)) > 0) {
                                                context.write(new Text("TEXT " + String.valueOf(doc_id) + " " + String.valueOf(idx)),
                                                        new DoubleWritable(Math.log(Config.N / df.get(word))));
                                            } else {
                                                context.write(new Text("TEXT " + String.valueOf(doc_id) + " " + String.valueOf(idx)),
                                                        new DoubleWritable(0));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            } else if(parts.length == 2){
                ArrayList<String> title = new ArrayList<>(Arrays.asList(parts[1].split(" ")));

                if (idxQueries != null) {
                    for(String word: title) {
                        for (int idx : idxQueries) {
                            String query = queries.get(idx);
                            if (query != null) {
                                for (String w_q : query.split(" ")) {
                                    if (w_q.equals(word)) {
                                        if (df.containsKey(word)) {
                                            if (Math.log(Config.N / df.get(word)) > 0) {
                                                context.write(new Text("TITLE " + String.valueOf(doc_id) + " " + String.valueOf(idx)),
                                                        new DoubleWritable(Math.log(Config.N / df.get(word))));
                                            } else {
                                                context.write(new Text("TITLE " + String.valueOf(doc_id) + " " + String.valueOf(idx)),
                                                        new DoubleWritable(0));
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


    public static class AllWordsReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        Map<Integer, String> queries = new HashMap<>();

        @Override
        protected void setup(Reducer.Context context) throws IOException {
            Path queriesFile = new Path(Config.QUERIES);
            FileSystem fs = queriesFile.getFileSystem(context.getConfiguration());
            FSDataInputStream file = fs.open(queriesFile);

            BufferedReader reader = new BufferedReader(new InputStreamReader(file));
            String query = reader.readLine();
            while (query != null && !query.equals("")) {
                queries.put(Integer.valueOf(query.split("\t")[0]), query.split("\t")[1]);
                query = reader.readLine();
            }
            reader.close();
        }

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            int idx = Integer.valueOf(key.toString().split(" ")[2]);
            int lenQuery = queries.get(idx).split(" ").length;
            double score = 0;
            int n = 0;

            for(DoubleWritable val: values){
                score = score + val.get();
                n++;
            }

            if((lenQuery-n) > 0){
                score = score * Math.pow(0.03, lenQuery-n);
            }

            context.write(key, new DoubleWritable(score));
        }
    }

    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new AllWordsIDF(), args);
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
        job.setJobName("ALL_WORDS_IDF");

        fs = FileSystem.get(getConf());
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }

        job.setJarByClass(AllWordsIDF.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(AllWordsMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setReducerClass(AllWordsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setNumReduceTasks(Config.REDUCE_COUNT);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }
}
