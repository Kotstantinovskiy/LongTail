import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

public class ProbLastClick {

    private String type = "QueryUrl";
    private HashSet<String> trainSet, testSet;
    private HashMap<String, Integer> urlId, queryId;

    ProbLastClick(HashSet<String> trainSet,
                      HashSet<String> testSet,
                      HashMap<String, Integer> urlId,
                      HashMap<String, Integer> queryId,
                      String type){
        if(type.equals("Url")){
            this.type = type;
        }
        this.urlId = urlId;
        this.queryId = queryId;
        this.trainSet = trainSet;
        this.testSet = testSet;
    }

    public void map(Mapper.Context context, SERP serp) throws IOException, InterruptedException {
        String lastClickUrl = serp.clicked.get(serp.clicked.size() - 1);

        if(trainSet.contains(lastClickUrl) || testSet.contains(lastClickUrl)) {
            for (String link : serp.urls) {
                if (trainSet.contains(link) || testSet.contains(link)) {
                    if (this.type.equals("QueryUrl")) {
                        if (lastClickUrl.equals(link)) {
                            if(queryId.containsKey(serp.query) && urlId.containsKey(link)) {
                                context.write(new Text(Config.PROB_LAST + Config.DELIMER + queryId.get(serp.query) + Config.DELIMER + urlId.get(link)),
                                        new Text("1"));
                            }
                        } else {
                            if(queryId.containsKey(serp.query) && urlId.containsKey(link)) {
                                context.write(new Text(Config.PROB_LAST + Config.DELIMER + queryId.get(serp.query) + Config.DELIMER + urlId.get(link)),
                                        new Text("0"));
                            }
                        }
                    } else {
                        if (lastClickUrl.equals(link)) {
                            if(urlId.containsKey(link)) {
                                context.write(new Text(Config.PROB_LAST + Config.DELIMER + urlId.get(link)),
                                        new Text("1"));
                            }
                        } else {
                            if(urlId.containsKey(link)) {
                                context.write(new Text(Config.PROB_LAST + Config.DELIMER + urlId.get(link)),
                                        new Text("0"));
                            }
                        }
                    }
                }
            }
        }
    }

    public void reduce(Text key,
                       Iterable<Text> values,
                       Reducer.Context context) throws IOException, InterruptedException {
        int show = 0, isLast = 0;
        for(Text value : values){
            show++;
            isLast = isLast + Integer.valueOf(value.toString());
        }

        double prob = 0;
        if(show != 0) {
            prob = (double) (isLast / show);
        }

        if(this.type.equals("QueryUrl")) {
            int query_id = Integer.valueOf(key.toString().split(Config.DELIMER)[1]);
            int url_id = Integer.valueOf(key.toString().split(Config.DELIMER)[2]);
            context.write(new Text("QueryUrl\t"
                            + key.toString().split(Config.DELIMER)[0] + "\t"
                            + String.valueOf(query_id) + "\t"
                            + String.valueOf(url_id)),
                    new Text(String.valueOf(prob)));
        } else {
            int url_id = Integer.valueOf(key.toString().split(Config.DELIMER)[1]);
            context.write(new Text("Url\t"
                            + key.toString().split(Config.DELIMER)[0] + "\t"
                            + String.valueOf(url_id)),
                    new Text(String.valueOf(prob)));
        }
    }
}
