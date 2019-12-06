import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

public class DBN {
    private String type = "QueryUrl";
    private HashSet<String> trainSet, testSet;
    private HashMap<String, Integer> urlId, queryId;

    DBN(HashSet<String> trainSet,
        HashSet<String> testSet,
        HashMap<String, Integer> urlId,
        HashMap<String, Integer> queryId,
        String type){
        if(type.equals("Url")){
            this.type = type;
        }
        this.trainSet = trainSet;
        this.testSet = testSet;
        this.urlId = urlId;
        this.queryId = queryId;
    }

    public void map(Mapper.Context context, SERP serp) throws IOException, InterruptedException {

        String lastClickedUrl = serp.clicked.get(serp.clicked.size() - 1);

        HashMap<String, Long> a_D = new HashMap<>();
        HashMap<String, Long> a_N = new HashMap<>();
        HashMap<String, Long> s_D = new HashMap<>();
        HashMap<String, Long> s_N = new HashMap<>();

        for(String url : serp.urls){
            if (url.equals(lastClickedUrl)) {
                a_D.put(url, (long) 1);
                break;
            } else {
                a_D.put(url, (long) 1);
            }

        }

        for (String clickedUrl : serp.clicked) {
            a_N.put(clickedUrl, (long) 1);
            s_D.put(clickedUrl, (long) 1);

        }

        s_N.put(lastClickedUrl, (long) 1);

        if(this.type.equals("QueryUrl")) {
            for (String link : serp.urls) {
                if(trainSet.contains(link) || testSet.contains(link)) {
                    if(queryId.containsKey(serp.query) && urlId.containsKey(link)) {
                        context.write(new Text(Config.DBN + Config.DELIMER + queryId.get(serp.query) + Config.DELIMER + urlId.get(link)),
                                new Text(Double.toString(a_N.getOrDefault(link, (long) 0)) +
                                        Config.DELIMER + Double.toString(a_D.getOrDefault(link, (long) 0)) +
                                        Config.DELIMER + Double.toString(s_N.getOrDefault(link, (long) 0)) +
                                        Config.DELIMER + Double.toString(s_D.getOrDefault(link, (long) 0))));
                    }
                }
            }
        } else {
            for (String link : serp.urls) {
                if(trainSet.contains(link) || testSet.contains(link)) {
                    if(urlId.containsKey(link)) {
                        context.write(new Text(Config.DBN + Config.DELIMER + urlId.get(link)),
                                new Text(Double.toString(a_N.getOrDefault(link, (long) 0)) +
                                        Config.DELIMER + Double.toString(a_D.getOrDefault(link, (long) 0)) +
                                        Config.DELIMER + Double.toString(s_N.getOrDefault(link, (long) 0)) +
                                        Config.DELIMER + Double.toString(s_D.getOrDefault(link, (long) 0))));
                    }
                }
            }
        }
    }

    public void reduce(Text key,
                       Iterable<Text> values,
                       Reducer.Context context) throws IOException, InterruptedException {

        double a_D = 0.0;
        double a_N = 0.0;
        double s_D = 0.0;
        double s_N = 0.0;

        final double alpha1 = 0.2;
        final double beta1 = 0.2;
        final double alpha2 = 0.2;
        final double beta2 = 0.2;

        for (Text value : values) {
            String[] parts = value.toString().split(Config.DELIMER);
            a_N += Double.parseDouble(parts[0]);
            a_D += Double.parseDouble(parts[1]);
            s_N += Double.parseDouble(parts[2]);
            s_D += Double.parseDouble(parts[3]);
        }

        if(this.type.equals("QueryUrl")) {
            int query_id = Integer.valueOf(key.toString().split(Config.DELIMER)[1]);
            int url_id = Integer.valueOf(key.toString().split(Config.DELIMER)[2]);
            context.write(new Text("QueryUrl\t"
                            + key.toString().split(Config.DELIMER)[0] + "\t"
                            + String.valueOf(query_id) + "\t"
                            + String.valueOf(url_id)),
                    new Text(String.valueOf((a_N + alpha1) / (a_D + alpha1 + beta1)) + "\t"
                            + String.valueOf((s_N + alpha2) / (s_D + alpha2 + beta2))));

        } else {
            int url_id = Integer.valueOf(key.toString().split(Config.DELIMER)[1]);
            context.write(new Text("Url\t"
                            + key.toString().split(Config.DELIMER)[0] + "\t"
                            + String.valueOf(url_id)),
                    new Text(String.valueOf((a_N + alpha1) / (a_D + alpha1 + beta1)) + "\t"
                            + String.valueOf((s_N + alpha2) / (s_D + alpha2 + beta2))));
        }
    }
}
