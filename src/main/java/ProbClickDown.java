import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

public class ProbClickDown {

    private String type = "QueryUrl";
    private HashSet<String> trainSet, testSet;
    private HashMap<String, Integer> urlId, queryId;

    ProbClickDown(HashSet<String> trainSet,
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
        int idx = 0;

        for (String link : serp.urls) {
            if(idx == serp.urls.size()-1){
                break;
            }

            if(trainSet.contains(link) || testSet.contains(link)) {
                String downUrl = serp.urls.get(idx+1);
                if(this.type.equals("QueryUrl")) {
                    if (serp.clicked.contains(downUrl)) {
                        if(queryId.containsKey(serp.query) && urlId.containsKey(link)) {
                            context.write(new Text(Config.PROB_DOWN + Config.DELIMER + queryId.get(serp.query) + Config.DELIMER + urlId.get(link)),
                                    new Text("1"));
                        }
                    } else {
                        if(queryId.containsKey(serp.query) && urlId.containsKey(link)) {
                            context.write(new Text(Config.PROB_DOWN + Config.DELIMER + queryId.get(serp.query) + Config.DELIMER + urlId.get(link)),
                                    new Text("0"));
                        }
                    }
                } else {
                    if (serp.clicked.contains(downUrl)) {
                        if(urlId.containsKey(link)) {
                            context.write(new Text(Config.PROB_DOWN + Config.DELIMER + urlId.get(link)),
                                    new Text("1"));
                        }
                    } else {
                        if(urlId.containsKey(link)) {
                            context.write(new Text(Config.PROB_DOWN + Config.DELIMER + urlId.get(link)),
                                    new Text("0"));
                        }
                    }
                }
            }
            idx++;
        }
    }

    public void reduce(Text key,
                       Iterable<Text> values,
                       Reducer.Context context) throws IOException, InterruptedException {
        double show = 0, isClicked = 0;
        for(Text value : values){
            show++;
            isClicked = isClicked + Integer.valueOf(value.toString());
        }

        double prob = 0;
        if(show != 0) {
            prob = isClicked / show;
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
