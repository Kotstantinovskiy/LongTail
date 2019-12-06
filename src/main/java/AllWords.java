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
import java.util.*;

public class AllWords extends Configured implements Tool {

    public static class AllWordsMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        Map<Integer, String> queries = new HashMap<>();
        Map<Integer, ArrayList<Integer>> docQueries =  new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            //System.out.println("Start End mapper");
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

            Path docQueriesFile = new Path(Config.DOC_QUERIES);
            file = fs.open(docQueriesFile);
            reader = new BufferedReader(new InputStreamReader(file));
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
        }

        @Override
        public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
            String[] parts_1 = line.toString().split("\t");

            int doc_id = Integer.valueOf(parts_1[0].split(" ")[0]);

            if(parts_1.length == 2) {
                String title = parts_1[0].split(" ")[1];
                String text = "";
            } else if(parts_1.length == 3) {
                String title = parts_1[0].split(" ")[1];
                String text = parts_1[0].split(" ")[2];
            }
            ArrayList<Integer> idxQueries = docQueries.get(doc_id);

            if (idxQueries != null) {

                    for (int idx : idxQueries) {
                        String query = queries.get(idx);
                        if (query != null) {
                            for (String w_q : query.split(" ")) {
                                if (w_q.equals(word)) {
                                    context.write(new Text("TEXT " + String.valueOf(doc) + " " + String.valueOf(idx)),
                                            new DoubleWritable(val));
                                }
                            }
                        }
                    }

                    for (int idx : idxQueries) {
                        String query = queries.get(idx);
                        if (query != null) {
                            for (String w : query.split(" ")) {
                                if (w.equals(word)) {
                                    context.write(new Text("TITLE " + String.valueOf(doc) + " " + String.valueOf(idx)),
                                            new DoubleWritable(val));
                                }
                            }
                        }
                    }
                }
            }
    }

    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new AllWords(), args);
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

        //Third stage
        System.out.println("Third stage");
        Job job = Job.getInstance(getConf());
        job.setJobName("THIRD_STAGE");

        fs = FileSystem.get(getConf());
        if (fs.exists(new Path(args[1] + "_3"))) {
            fs.delete(new Path(args[1] + "_3"), true);
        }

        job.setJarByClass(TF_ICF.class);
        FileInputFormat.setInputPaths(job, new Path(args[1] + "_2/part-*"));
        FileOutputFormat.setOutputPath(job, new Path(args[1] + "_3"));

        job.setMapperClass(AllWords.AllWordsMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setNumReduceTasks(0);

        success = job.waitForCompletion(true);

        return success ? 0 : 1;
    }
}
