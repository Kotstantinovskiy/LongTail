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
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;

public class ExctractFeaturesDomain extends Configured implements Tool {

    public static class AllDQMapper extends Mapper<LongWritable, Text, Text, Text> {

        HashSet<String> trainSet = new HashSet<>();
        HashSet<String> testSet = new HashSet<>();
        HashSet<String> trainDomain = new HashSet<>();
        HashSet<String> testDomain = new HashSet<>();
        HashMap<String, Integer> queryId = new HashMap<>();
        HashMap<String, Integer> urlId = new HashMap<>();
        HashMap<String, Integer> domainId = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException, NullPointerException{
            System.out.println("Start mapper");

            Path trainPath = new Path(Config.PATH_TRAIN);
            Path testPath = new Path(Config.PATH_TEST);
            Path trainDomainPath = new Path(Config.PATH_DOMAIN_TRAIN);
            Path testDomainPath = new Path(Config.PATH_DOMAIN_TEST);
            Path idQueryPath = new Path(Config.PATH_ID_QUERY);
            Path idUrlPath = new Path(Config.PATH_ID_URL);
            Path idDomainPath = new Path(Config.PATH_ID_DOMAIN);

            FileSystem fs = trainPath.getFileSystem(context.getConfiguration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(trainPath), StandardCharsets.UTF_8));
            String str;
            while ((str = br.readLine()) != null) {
                str = str.trim()
                        .replace("http://", "")
                        .replace("https://", "")
                        .replace("www.", "");
                trainSet.add(str);
            }
            br.close();

            br = new BufferedReader(new InputStreamReader(fs.open(testPath), StandardCharsets.UTF_8));
            while ((str = br.readLine()) != null) {
                str = str.trim()
                        .replace("http://", "")
                        .replace("https://", "")
                        .replace("www.", "");
                testSet.add(str);
            }
            br.close();

            br = new BufferedReader(new InputStreamReader(fs.open(trainDomainPath), StandardCharsets.UTF_8));
            while ((str = br.readLine()) != null) {
                str = str.trim()
                        .replace("http://", "")
                        .replace("https://", "")
                        .replace("www.", "");
                trainDomain.add(str);
            }
            br.close();

            br = new BufferedReader(new InputStreamReader(fs.open(testDomainPath), StandardCharsets.UTF_8));
            while ((str = br.readLine()) != null) {
                str = str.trim()
                        .replace("http://", "")
                        .replace("https://", "")
                        .replace("www.", "");
                testDomain.add(str);
            }
            br.close();

            br = new BufferedReader(new InputStreamReader(fs.open(idUrlPath), StandardCharsets.UTF_8));
            while ((str = br.readLine()) != null) {
                String[] id_url = str.split("\t");
                id_url[1] = id_url[1].trim()
                        .replace("http://", "")
                        .replace("https://", "")
                        .replace("www.", "");
                urlId.put(id_url[1], Integer.valueOf(id_url[0]));
            }
            br.close();

            br = new BufferedReader(new InputStreamReader(fs.open(idQueryPath), StandardCharsets.UTF_8));
            while ((str = br.readLine()) != null) {
                String[] id_query = str.split("\t");
                id_query[1] = id_query[1].trim()
                        .replace("http://", "")
                        .replace("https://", "")
                        .replace("www.", "");
                queryId.put(id_query[1], Integer.valueOf(id_query[0]));
            }
            br.close();

            br = new BufferedReader(new InputStreamReader(fs.open(idDomainPath), StandardCharsets.UTF_8));
            while ((str = br.readLine()) != null) {
                String[] id_domain = str.split("\t");
                id_domain[1] = id_domain[1].trim()
                        .replace("http://", "")
                        .replace("https://", "")
                        .replace("www.", "");
                domainId.put(id_domain[1], Integer.valueOf(id_domain[0]));
            }
            br.close();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            SERP serp = new SERP(value.toString());

            ClicksDQ clicksDQ = new ClicksDQ(trainDomain, testDomain, domainId, queryId);
            ImpDQ impDQ = new ImpDQ(trainDomain, testDomain, domainId, queryId);
            CTRDQ ctrDQ = new CTRDQ(trainDomain, testDomain, domainId, queryId);
            FirstCTRDQ firstCTRDQ = new FirstCTRDQ(trainDomain, testDomain, domainId, queryId);
            LastCTRDQ lastCTRDQ = new LastCTRDQ(trainDomain, testDomain, domainId, queryId);
            MeanCountClickUpDQ meanCountClickUpDQ = new MeanCountClickUpDQ(trainDomain, testDomain, domainId, queryId);
            MeanNumClickDQ meanNumClickDQ = new MeanNumClickDQ(trainDomain, testDomain, domainId, queryId);
            MeanPositionClickDQ meanPositionClickDQ = new MeanPositionClickDQ(trainDomain, testDomain, domainId, queryId);
            MeanPositionDQ meanPositionDQ = new MeanPositionDQ(trainDomain, testDomain, domainId, queryId);
            MeanTimeDQ meanTimeDQ = new MeanTimeDQ(trainDomain, testDomain, domainId, queryId);
            OnlyCTRDQ onlyCTRDQ = new OnlyCTRDQ(trainDomain, testDomain, domainId, queryId);
            ProbClickDownDQ probClickDownDQ = new ProbClickDownDQ(trainDomain, testDomain, domainId, queryId);
            ProbClickUpDQ probClickUpDQ = new ProbClickUpDQ(trainDomain, testDomain, domainId, queryId);
            ProbLastClickDQ probLastClickDQ = new ProbLastClickDQ(trainDomain, testDomain, domainId, queryId);
            DBNDQ dbnDQ = new DBNDQ(trainDomain, testDomain, domainId, queryId);

            impDQ.map(context, serp);
            clicksDQ.map(context, serp);
            ctrDQ.map(context, serp);
            firstCTRDQ.map(context, serp);
            lastCTRDQ.map(context, serp);
            onlyCTRDQ.map(context, serp);
            meanCountClickUpDQ.map(context, serp);
            meanNumClickDQ.map(context, serp);
            meanPositionDQ.map(serp, context);
            meanPositionClickDQ.map(context, serp);
            meanTimeDQ.map(context, serp);
            probClickDownDQ.map(context, serp);
            probClickUpDQ.map(context, serp);
            probLastClickDQ.map(context, serp);
            dbnDQ.map(context, serp);
        }
    }

    public static class AllDQReducer extends Reducer<Text, Text, Text, Text> {

        HashSet<String> trainSet = new HashSet<>();
        HashSet<String> testSet = new HashSet<>();
        HashSet<String> trainDomain = new HashSet<>();
        HashSet<String> testDomain = new HashSet<>();
        HashMap<String, Integer> queryId = new HashMap<>();
        HashMap<String, Integer> urlId = new HashMap<>();
        HashMap<String, Integer> domainId = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException, NullPointerException{
            System.out.println("Start reducer");

            Path trainPath = new Path(Config.PATH_TRAIN);
            Path testPath = new Path(Config.PATH_TEST);
            Path trainDomainPath = new Path(Config.PATH_DOMAIN_TRAIN);
            Path testDomainPath = new Path(Config.PATH_DOMAIN_TEST);
            Path idQueryPath = new Path(Config.PATH_ID_QUERY);
            Path idUrlPath = new Path(Config.PATH_ID_URL);
            Path idDomainPath = new Path(Config.PATH_ID_DOMAIN);

            FileSystem fs = trainPath.getFileSystem(context.getConfiguration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(trainPath), StandardCharsets.UTF_8));
            String str;
            while ((str = br.readLine()) != null) {
                str = str.trim()
                        .replace("http://", "")
                        .replace("https://", "")
                        .replace("www.", "");
                trainSet.add(str);
            }
            br.close();

            br = new BufferedReader(new InputStreamReader(fs.open(testPath), StandardCharsets.UTF_8));
            while ((str = br.readLine()) != null) {
                str = str.trim()
                        .replace("http://", "")
                        .replace("https://", "")
                        .replace("www.", "");
                testSet.add(str);
            }
            br.close();

            br = new BufferedReader(new InputStreamReader(fs.open(trainDomainPath), StandardCharsets.UTF_8));
            while ((str = br.readLine()) != null) {
                str = str.trim()
                        .replace("http://", "")
                        .replace("https://", "")
                        .replace("www.", "");
                trainDomain.add(str);
            }
            br.close();

            br = new BufferedReader(new InputStreamReader(fs.open(testDomainPath), StandardCharsets.UTF_8));
            while ((str = br.readLine()) != null) {
                str = str.trim()
                        .replace("http://", "")
                        .replace("https://", "")
                        .replace("www.", "");
                testDomain.add(str);
            }
            br.close();

            br = new BufferedReader(new InputStreamReader(fs.open(idUrlPath), StandardCharsets.UTF_8));
            while ((str = br.readLine()) != null) {
                String[] id_url = str.split("\t");
                id_url[1] = id_url[1].trim()
                        .replace("http://", "")
                        .replace("https://", "")
                        .replace("www.", "");
                urlId.put(id_url[1], Integer.valueOf(id_url[0]));
            }
            br.close();

            br = new BufferedReader(new InputStreamReader(fs.open(idQueryPath), StandardCharsets.UTF_8));
            while ((str = br.readLine()) != null) {
                String[] id_query = str.split("\t");
                id_query[1] = id_query[1].trim()
                        .replace("http://", "")
                        .replace("https://", "")
                        .replace("www.", "");
                queryId.put(id_query[1], Integer.valueOf(id_query[0]));
            }
            br.close();

            br = new BufferedReader(new InputStreamReader(fs.open(idDomainPath), StandardCharsets.UTF_8));
            while ((str = br.readLine()) != null) {
                String[] id_domain = str.split("\t");
                id_domain[1] = id_domain[1].trim()
                        .replace("http://", "")
                        .replace("https://", "")
                        .replace("www.", "");
                domainId.put(id_domain[1], Integer.valueOf(id_domain[0]));
            }
            br.close();
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ClicksDQ clicksDQ = new ClicksDQ(trainDomain, testDomain, domainId, queryId);
            ImpDQ impDQ = new ImpDQ(trainDomain, testDomain, domainId, queryId);
            CTRDQ ctrDQ = new CTRDQ(trainDomain, testDomain, domainId, queryId);
            FirstCTRDQ firstCTRDQ = new FirstCTRDQ(trainDomain, testDomain, domainId, queryId);
            LastCTRDQ lastCTRDQ = new LastCTRDQ(trainDomain, testDomain, domainId, queryId);
            MeanCountClickUpDQ meanCountClickUpDQ = new MeanCountClickUpDQ(trainDomain, testDomain, domainId, queryId);
            MeanNumClickDQ meanNumClickDQ = new MeanNumClickDQ(trainDomain, testDomain, domainId, queryId);
            MeanPositionClickDQ meanPositionClickDQ = new MeanPositionClickDQ(trainDomain, testDomain, domainId, queryId);
            MeanPositionDQ meanPositionDQ = new MeanPositionDQ(trainDomain, testDomain, domainId, queryId);
            MeanTimeDQ meanTimeDQ = new MeanTimeDQ(trainDomain, testDomain, domainId, queryId);
            OnlyCTRDQ onlyCTRDQ = new OnlyCTRDQ(trainDomain, testDomain, domainId, queryId);
            ProbClickDownDQ probClickDownDQ = new ProbClickDownDQ(trainDomain, testDomain, domainId, queryId);
            ProbClickUpDQ probClickUpDQ = new ProbClickUpDQ(trainDomain, testDomain, domainId, queryId);
            ProbLastClickDQ probLastClickDQ = new ProbLastClickDQ(trainDomain, testDomain, domainId, queryId);
            DBNDQ dbnDQ = new DBNDQ(trainDomain, testDomain, domainId, queryId);

            if(key.toString().split(Config.DELIMER)[0].equals(Config.CLICK_DQ) && key.toString().split(Config.DELIMER).length == 3){
                clicksDQ.reduce(key, values, context);
            } else if(key.toString().split(Config.DELIMER)[0].equals(Config.IMP_DQ) && key.toString().split(Config.DELIMER).length == 3){
                impDQ.reduce(key, values, context);
            } else if(key.toString().split(Config.DELIMER)[0].equals(Config.CTR_DQ) && key.toString().split(Config.DELIMER).length == 3){
                ctrDQ.reduce(key, values, context);
            } else if(key.toString().split(Config.DELIMER)[0].equals(Config.FIRST_CTR_DQ) && key.toString().split(Config.DELIMER).length == 3){
                firstCTRDQ.reduce(key, values, context);
            } else if(key.toString().split(Config.DELIMER)[0].equals(Config.LAST_CTR_DQ) && key.toString().split(Config.DELIMER).length == 3){
                lastCTRDQ.reduce(key, values, context);
            } else if(key.toString().split(Config.DELIMER)[0].equals(Config.MEAN_COUNT_CLICK_UP_DQ) && key.toString().split(Config.DELIMER).length == 3){
                meanCountClickUpDQ.reduce(key, values, context);
            } else if(key.toString().split(Config.DELIMER)[0].equals(Config.MEAN_NUM_CLICK_DQ) && key.toString().split(Config.DELIMER).length == 3){
                meanNumClickDQ.reduce(key, values, context);
            } else if(key.toString().split(Config.DELIMER)[0].equals(Config.MEAN_POSITION_CLICK_DQ) && key.toString().split(Config.DELIMER).length == 3){
                meanPositionClickDQ.reduce(key, values, context);
            } else if(key.toString().split(Config.DELIMER)[0].equals(Config.MEAN_POSITION_DQ) && key.toString().split(Config.DELIMER).length == 3){
                meanPositionDQ.reduce(key, values, context);
            } else if(key.toString().split(Config.DELIMER)[0].equals(Config.MEAN_TIME_DQ) && key.toString().split(Config.DELIMER).length == 3){
                meanTimeDQ.reduce(key, values, context);
            } else if(key.toString().split(Config.DELIMER)[0].equals(Config.ONLY_CTR_DQ) && key.toString().split(Config.DELIMER).length == 3){
                onlyCTRDQ.reduce(key, values, context);
            } else if(key.toString().split(Config.DELIMER)[0].equals(Config.PROB_DOWN_DQ) && key.toString().split(Config.DELIMER).length == 3){
                probClickDownDQ.reduce(key, values, context);
            } else if(key.toString().split(Config.DELIMER)[0].equals(Config.PROB_UPPER_DQ) && key.toString().split(Config.DELIMER).length == 3){
                probClickUpDQ.reduce(key, values, context);
            }else if(key.toString().split(Config.DELIMER)[0].equals(Config.PROB_LAST_DQ) && key.toString().split(Config.DELIMER).length == 3){
                probLastClickDQ.reduce(key, values, context);
            } else if(key.toString().split(Config.DELIMER)[0].equals(Config.DBN_DQ) && key.toString().split(Config.DELIMER).length == 3){
                dbnDQ.reduce(key, values, context);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new ExctractFeaturesDomain(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.out.println("Two parameters are required!");
            return -1;
        }

        Job job = Job.getInstance(getConf());
        job.setJobName("ExctractFeatures_2");

        FileSystem fs = FileSystem.get(getConf());
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }

        job.setJarByClass(ExctractFeatures.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(AllDQMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(AllDQReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(Config.REDUCE_COUNT);

        boolean success = job.waitForCompletion(true);

        return success ? 0 : 1;
    }
}