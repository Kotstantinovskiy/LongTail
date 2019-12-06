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

public class ExctractFeatures extends Configured implements Tool {

    public static class AllMapper extends Mapper<LongWritable, Text, Text, Text> {

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

            Imp imp_u = new Imp(trainSet, testSet, urlId, queryId, "Url");
            Imp imp_qu = new Imp(trainSet, testSet, urlId, queryId, "QueryUrl");
            Clicks clicks_u = new Clicks(trainSet, testSet, urlId, queryId, "Url");
            Clicks clicks_qu = new Clicks(trainSet, testSet, urlId, queryId, "QueryUrl");
            CTR ctr_u = new CTR(trainSet, testSet, urlId, queryId, "Url");
            CTR ctr_qu = new CTR(trainSet, testSet, urlId, queryId, "QueryUrl");
            FirstCTR firstCTR_u = new FirstCTR(trainSet, testSet, urlId, queryId, "Url");
            FirstCTR firstCTR_qu = new FirstCTR(trainSet, testSet, urlId, queryId, "QueryUrl");
            LastCTR lastCTR_u = new LastCTR(trainSet, testSet, urlId, queryId, "Url");
            LastCTR lastCTR_qu = new LastCTR(trainSet, testSet, urlId, queryId, "QueryUrl");
            OnlyCTR onlyCTR_u = new OnlyCTR(trainSet, testSet, urlId, queryId, "Url");
            OnlyCTR onlyCTR_qu = new OnlyCTR(trainSet, testSet, urlId, queryId, "QueryUrl");
            DBN dbn_u = new DBN(trainSet, testSet, urlId, queryId, "Url");
            DBN dbn_qu = new DBN(trainSet, testSet, urlId, queryId, "QueryUrl");
            MeanCountClickUp meanCountClickUp_u = new MeanCountClickUp(trainSet, testSet, urlId, queryId, "Url");
            MeanCountClickUp meanCountClickUp_qu = new MeanCountClickUp(trainSet, testSet, urlId, queryId, "QueryUrl");
            MeanNumClick meanNumClick_u = new MeanNumClick(trainSet, testSet, urlId, queryId, "Url");
            MeanNumClick meanNumClick_qu = new MeanNumClick(trainSet, testSet, urlId, queryId, "QueryUrl");
            MeanPosition meanPosition_u = new MeanPosition(trainSet, testSet, urlId, queryId, "Url");
            MeanPosition meanPosition_qu = new MeanPosition(trainSet, testSet, urlId, queryId, "QueryUrl");
            MeanPositionClick meanPositionClick_u = new MeanPositionClick(trainSet, testSet, urlId, queryId, "Url");
            MeanPositionClick meanPositionClick_qu = new MeanPositionClick(trainSet, testSet, urlId, queryId, "QueryUrl");
            MeanTime meanTime_u = new MeanTime(trainSet, testSet, urlId, queryId, "Url");
            MeanTime meanTime_qu = new MeanTime(trainSet, testSet, urlId, queryId, "QueryUrl");
            ProbClickDown probClickDown_u = new ProbClickDown(trainSet, testSet, urlId, queryId, "Url");
            ProbClickDown probClickDown_qu = new ProbClickDown(trainSet, testSet, urlId, queryId, "QueryUrl");
            ProbClickUp probClickUp_u = new ProbClickUp(trainSet, testSet, urlId, queryId, "Url");
            ProbClickUp probClickUp_qu = new ProbClickUp(trainSet, testSet, urlId, queryId, "QueryUrl");
            ProbLastClick probLastClick_u = new ProbLastClick(trainSet, testSet, urlId, queryId, "Url");
            ProbLastClick probLastClick_qu = new ProbLastClick(trainSet, testSet, urlId, queryId, "QueryUrl");

            ImpDomain imp_domain = new ImpDomain(trainDomain, testDomain, domainId);
            ClicksDomain click_domain = new ClicksDomain(trainDomain, testDomain, domainId);
            CTRDomain ctr_domain = new CTRDomain(trainDomain, testDomain, domainId);
            FirstCTRDomain first_ctr_domain = new FirstCTRDomain(trainDomain, testDomain, domainId);
            LastCTRDomain last_ctr_domain = new LastCTRDomain(trainDomain, testDomain, domainId);
            MeanPositionClickDomain mean_position_click_domain = new MeanPositionClickDomain(trainDomain, testDomain, domainId);
            MeanTimeDomain mean_time_domain = new MeanTimeDomain(trainDomain, testDomain, domainId);
            OnlyCTRDomain only_ctr_domain = new OnlyCTRDomain(trainDomain, testDomain, domainId);

            imp_u.map(context, serp);
            imp_qu.map(context, serp);
            clicks_u.map(context, serp);
            clicks_qu.map(context, serp);
            ctr_u.map(context, serp);
            ctr_qu.map(context, serp);
            firstCTR_u.map(context, serp);
            firstCTR_qu.map(context, serp);
            lastCTR_u.map(context, serp);
            lastCTR_qu.map(context, serp);
            onlyCTR_u.map(context, serp);
            onlyCTR_qu.map(context, serp);
            dbn_u.map(context, serp);
            dbn_qu.map(context, serp);
            meanCountClickUp_u.map(context, serp);
            meanCountClickUp_qu.map(context, serp);
            meanNumClick_u.map(context, serp);
            meanNumClick_qu.map(context, serp);
            meanPosition_u.map(serp, context);
            meanPosition_qu.map(serp, context);
            meanPositionClick_u.map(serp, context);
            meanPositionClick_qu.map(serp, context);
            meanTime_u.map(context, serp);
            meanTime_qu.map(context, serp);
            probClickDown_u.map(context, serp);
            probClickDown_qu.map(context, serp);
            probClickUp_u.map(context, serp);
            probClickUp_qu.map(context, serp);
            probLastClick_u.map(context, serp);
            probLastClick_qu.map(context, serp);

            imp_domain.map(context, serp);
            click_domain.map(context, serp);
            ctr_domain.map(context, serp);
            first_ctr_domain.map(context, serp);
            last_ctr_domain.map(context, serp);
            mean_position_click_domain.map(context, serp);
            mean_time_domain.map(context, serp);
            only_ctr_domain.map(context, serp);
        }
    }

    public static class AllReducer extends Reducer<Text, Text, Text, Text> {

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
            Imp imp_u = new Imp(trainSet, testSet, urlId, queryId, "Url");
            Imp imp_qu = new Imp(trainSet, testSet, urlId, queryId, "QueryUrl");
            Clicks clicks_u = new Clicks(trainSet, testSet, urlId, queryId, "Url");
            Clicks clicks_qu = new Clicks(trainSet, testSet, urlId, queryId, "QueryUrl");
            CTR ctr_u = new CTR(trainSet, testSet, urlId, queryId, "Url");
            CTR ctr_qu = new CTR(trainSet, testSet, urlId, queryId, "QueryUrl");
            FirstCTR firstCTR_u = new FirstCTR(trainSet, testSet, urlId, queryId, "Url");
            FirstCTR firstCTR_qu = new FirstCTR(trainSet, testSet, urlId, queryId, "QueryUrl");
            LastCTR lastCTR_u = new LastCTR(trainSet, testSet, urlId, queryId, "Url");
            LastCTR lastCTR_qu = new LastCTR(trainSet, testSet, urlId, queryId, "QueryUrl");
            OnlyCTR onlyCTR_u = new OnlyCTR(trainSet, testSet, urlId, queryId, "Url");
            OnlyCTR onlyCTR_qu = new OnlyCTR(trainSet, testSet, urlId, queryId, "QueryUrl");
            DBN dbn_u = new DBN(trainSet, testSet, urlId, queryId, "Url");
            DBN dbn_qu = new DBN(trainSet, testSet, urlId, queryId, "QueryUrl");
            MeanCountClickUp meanCountClickUp_u = new MeanCountClickUp(trainSet, testSet, urlId, queryId, "Url");
            MeanCountClickUp meanCountClickUp_qu = new MeanCountClickUp(trainSet, testSet, urlId, queryId, "QueryUrl");
            MeanNumClick meanNumClick_u = new MeanNumClick(trainSet, testSet, urlId, queryId, "Url");
            MeanNumClick meanNumClick_qu = new MeanNumClick(trainSet, testSet, urlId, queryId, "QueryUrl");
            MeanPosition meanPosition_u = new MeanPosition(trainSet, testSet, urlId, queryId, "Url");
            MeanPosition meanPosition_qu = new MeanPosition(trainSet, testSet, urlId, queryId, "QueryUrl");
            MeanPositionClick meanPositionClick_u = new MeanPositionClick(trainSet, testSet, urlId, queryId, "Url");
            MeanPositionClick meanPositionClick_qu = new MeanPositionClick(trainSet, testSet, urlId, queryId, "QueryUrl");
            MeanTime meanTime_u = new MeanTime(trainSet, testSet, urlId, queryId, "Url");
            MeanTime meanTime_qu = new MeanTime(trainSet, testSet, urlId, queryId, "QueryUrl");
            ProbClickDown probClickDown_u = new ProbClickDown(trainSet, testSet, urlId, queryId, "Url");
            ProbClickDown probClickDown_qu = new ProbClickDown(trainSet, testSet, urlId, queryId, "QueryUrl");
            ProbClickUp probClickUp_u = new ProbClickUp(trainSet, testSet, urlId, queryId, "Url");
            ProbClickUp probClickUp_qu = new ProbClickUp(trainSet, testSet, urlId, queryId, "QueryUrl");
            ProbLastClick probLastClick_u = new ProbLastClick(trainSet, testSet, urlId, queryId, "Url");
            ProbLastClick probLastClick_qu = new ProbLastClick(trainSet, testSet, urlId, queryId, "QueryUrl");

            ImpDomain imp_domain = new ImpDomain(trainDomain, testDomain, domainId);
            ClicksDomain click_domain = new ClicksDomain(trainDomain, testDomain, domainId);
            CTRDomain ctr_domain = new CTRDomain(trainDomain, testDomain, domainId);
            FirstCTRDomain first_ctr_domain = new FirstCTRDomain(trainDomain, testDomain, domainId);
            LastCTRDomain last_ctr_domain = new LastCTRDomain(trainDomain, testDomain, domainId);
            MeanPositionClickDomain mean_position_click_domain = new MeanPositionClickDomain(trainDomain, testDomain, domainId);
            MeanTimeDomain mean_time_domain = new MeanTimeDomain(trainDomain, testDomain, domainId);
            OnlyCTRDomain only_ctr_domain = new OnlyCTRDomain(trainDomain, testDomain, domainId);

            if(key.toString().split(Config.DELIMER)[0].equals(Config.IMP) && key.toString().split(Config.DELIMER).length == 2){
                imp_u.reduce(key, values, context);
            } else if(key.toString().split(Config.DELIMER)[0].equals(Config.IMP) && key.toString().split(Config.DELIMER).length == 3){
                imp_qu.reduce(key, values, context);
            } else if(key.toString().split(Config.DELIMER)[0].equals(Config.CLICK) && key.toString().split(Config.DELIMER).length == 2){
                clicks_u.reduce(key, values, context);
            } else if(key.toString().split(Config.DELIMER)[0].equals(Config.CLICK) && key.toString().split(Config.DELIMER).length == 3){
                clicks_qu.reduce(key, values, context);
            } else if(key.toString().split(Config.DELIMER)[0].equals(Config.CTR) && key.toString().split(Config.DELIMER).length == 2){
                ctr_u.reduce(key, values, context);
            } else if(key.toString().split(Config.DELIMER)[0].equals(Config.CTR) && key.toString().split(Config.DELIMER).length == 3){
                ctr_qu.reduce(key, values, context);
            } else if(key.toString().split(Config.DELIMER)[0].equals(Config.FIRST_CTR) && key.toString().split(Config.DELIMER).length == 2){
                firstCTR_u.reduce(key, values, context);
            } else if(key.toString().split(Config.DELIMER)[0].equals(Config.FIRST_CTR) && key.toString().split(Config.DELIMER).length == 3){
                firstCTR_qu.reduce(key, values, context);
            } else if(key.toString().split(Config.DELIMER)[0].equals(Config.LAST_CTR) && key.toString().split(Config.DELIMER).length == 2){
                lastCTR_u.reduce(key, values, context);
            } else if(key.toString().split(Config.DELIMER)[0].equals(Config.LAST_CTR) && key.toString().split(Config.DELIMER).length == 3){
                lastCTR_qu.reduce(key, values, context);
            } else if(key.toString().split(Config.DELIMER)[0].equals(Config.ONLY_CTR) && key.toString().split(Config.DELIMER).length == 2){
                onlyCTR_u.reduce(key, values, context);
            } else if(key.toString().split(Config.DELIMER)[0].equals(Config.ONLY_CTR) && key.toString().split(Config.DELIMER).length == 3){
                onlyCTR_qu.reduce(key, values, context);
            } else if(key.toString().split(Config.DELIMER)[0].equals(Config.DBN) && key.toString().split(Config.DELIMER).length == 3){
                dbn_qu.reduce(key, values, context);
            } else if(key.toString().split(Config.DELIMER)[0].equals(Config.DBN) && key.toString().split(Config.DELIMER).length == 2){
                dbn_u.reduce(key, values, context);
            }else if(key.toString().split(Config.DELIMER)[0].equals(Config.MEAN_COUNT_CLICK_UP) && key.toString().split(Config.DELIMER).length == 3){
                meanCountClickUp_qu.reduce(key, values, context);
            } else if(key.toString().split(Config.DELIMER)[0].equals(Config.MEAN_COUNT_CLICK_UP) && key.toString().split(Config.DELIMER).length == 2){
                meanCountClickUp_u.reduce(key, values, context);
            } else if(key.toString().split(Config.DELIMER)[0].equals(Config.MEAN_NUM_CLICK) && key.toString().split(Config.DELIMER).length == 3){
                meanNumClick_qu.reduce(key, values, context);
            } else if(key.toString().split(Config.DELIMER)[0].equals(Config.MEAN_NUM_CLICK) && key.toString().split(Config.DELIMER).length == 2){
                meanNumClick_u.reduce(key, values, context);
            } else if(key.toString().split(Config.DELIMER)[0].equals(Config.MEAN_POSITION) && key.toString().split(Config.DELIMER).length == 3){
                meanPosition_qu.reduce(key, values, context);
            } else if(key.toString().split(Config.DELIMER)[0].equals(Config.MEAN_POSITION) && key.toString().split(Config.DELIMER).length == 2){
                meanPosition_u.reduce(key, values, context);
            } else if(key.toString().split(Config.DELIMER)[0].equals(Config.MEAN_POSITION_CLICK) && key.toString().split(Config.DELIMER).length == 3){
                meanPositionClick_qu.reduce(key, values, context);
            } else if(key.toString().split(Config.DELIMER)[0].equals(Config.MEAN_POSITION_CLICK) && key.toString().split(Config.DELIMER).length == 2){
                meanPositionClick_u.reduce(key, values, context);
            } else if(key.toString().split(Config.DELIMER)[0].equals(Config.MEAN_TIME) && key.toString().split(Config.DELIMER).length == 2){
                meanTime_u.reduce(key, values, context);
            } else if(key.toString().split(Config.DELIMER)[0].equals(Config.MEAN_TIME) && key.toString().split(Config.DELIMER).length == 3){
                meanTime_qu.reduce(key, values, context);
            } else if(key.toString().split(Config.DELIMER)[0].equals(Config.PROB_DOWN) && key.toString().split(Config.DELIMER).length == 3){
                probClickDown_qu.reduce(key, values, context);
            } else if(key.toString().split(Config.DELIMER)[0].equals(Config.PROB_DOWN) && key.toString().split(Config.DELIMER).length == 2){
                probClickDown_u.reduce(key, values, context);
            } else if(key.toString().split(Config.DELIMER)[0].equals(Config.PROB_UPPER) && key.toString().split(Config.DELIMER).length == 3){
                probClickUp_qu.reduce(key, values, context);
            } else if(key.toString().split(Config.DELIMER)[0].equals(Config.PROB_UPPER) && key.toString().split(Config.DELIMER).length == 2){
                probClickUp_u.reduce(key, values, context);
            } else if(key.toString().split(Config.DELIMER)[0].equals(Config.PROB_LAST) && key.toString().split(Config.DELIMER).length == 3){
                probLastClick_qu.reduce(key, values, context);
            } else if(key.toString().split(Config.DELIMER)[0].equals(Config.PROB_LAST) && key.toString().split(Config.DELIMER).length == 2){
                probLastClick_u.reduce(key, values, context);
            } else if((key.toString().split(Config.DELIMER)[0].equals(Config.CTR_DOMAIN_1) && key.toString().split(Config.DELIMER).length == 2) ||
                    (key.toString().split(Config.DELIMER)[0].equals(Config.CTR_DOMAIN_2) && key.toString().split(Config.DELIMER).length == 2)){
                ctr_domain.reduce(key, values, context);
            } else if(key.toString().split(Config.DELIMER)[0].equals(Config.IMP_DOMAIN) && key.toString().split(Config.DELIMER).length == 2){
                imp_domain.reduce(key, values, context);
            } else if(key.toString().split(Config.DELIMER)[0].equals(Config.CLICK_DOMAIN) && key.toString().split(Config.DELIMER).length == 2){
                click_domain.reduce(key, values, context);
            } else if(key.toString().split(Config.DELIMER)[0].equals(Config.FIRST_CTR_DOMAIN) && key.toString().split(Config.DELIMER).length == 2){
                first_ctr_domain.reduce(key, values, context);
            } else if(key.toString().split(Config.DELIMER)[0].equals(Config.LAST_CTR_DOMAIN) && key.toString().split(Config.DELIMER).length == 2){
                last_ctr_domain.reduce(key, values, context);
            } else if(key.toString().split(Config.DELIMER)[0].equals(Config.MEAN_POSITION_CLICK_DOMAIN) && key.toString().split(Config.DELIMER).length == 2){
                mean_position_click_domain.reduce(key, values, context);
            } else if(key.toString().split(Config.DELIMER)[0].equals(Config.MEAN_TIME_DOMAIN) && key.toString().split(Config.DELIMER).length == 2){
                mean_time_domain.reduce(key, values, context);
            } else if(key.toString().split(Config.DELIMER)[0].equals(Config.ONLY_CTR_DOMAIN) && key.toString().split(Config.DELIMER).length == 2){
                only_ctr_domain.reduce(key, values, context);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new ExctractFeatures(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.out.println("Two parameters are required!");
            return -1;
        }

        Job job = Job.getInstance(getConf());
        job.setJobName("ExctractFeatures");

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