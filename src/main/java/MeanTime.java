import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class MeanTime {

    private String type = "QueryUrl";
    private HashSet<String> trainSet, testSet;
    private HashMap<String, Integer> urlId, queryId;

    MeanTime(HashSet<String> trainSet,
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
        if(serp.timestamps.size() >= 2) {
            for (int i = 0; i < serp.timestamps.size() - 1; i++) {
                long delta_time = Math.abs(serp.timestamps.get(i) - serp.timestamps.get(i + 1));
                String url = serp.clicked.get(i);
                if(trainSet.contains(url) || testSet.contains(url)) {
                    if (this.type.equals("QueryUrl")) {
                        if(queryId.containsKey(serp.query) && urlId.containsKey(url)) {
                            context.write(new Text(Config.MEAN_TIME + Config.DELIMER + queryId.get(serp.query) + Config.DELIMER + urlId.get(url)),
                                    new Text(String.valueOf(delta_time)));
                        }
                    } else {
                        if(urlId.containsKey(url)) {
                            context.write(new Text(Config.MEAN_TIME + Config.DELIMER + urlId.get(url)),
                                    new Text(String.valueOf(delta_time)));
                        }
                    }
                }
            }
        }
    }

    public void reduce(Text key,
                       Iterable<Text> values,
                       Reducer.Context context) throws IOException, InterruptedException {
        double mean = 0, length = 0;
        for(Text value : values){
            length++;
            mean = mean + Math.log(1 + Double.valueOf(value.toString()));
        }

        mean = mean / length;

        if(this.type.equals("QueryUrl")) {
            int query_id = Integer.valueOf(key.toString().split(Config.DELIMER)[1]);
            int url_id = Integer.valueOf(key.toString().split(Config.DELIMER)[2]);
            context.write(new Text("QueryUrl\t"
                            + key.toString().split(Config.DELIMER)[0] + "\t"
                            + String.valueOf(query_id) + "\t"
                            + String.valueOf(url_id)),
                    new Text(String.valueOf(mean)));
        } else {
            int url_id = Integer.valueOf(key.toString().split(Config.DELIMER)[1]);
            context.write(new Text("Url\t"
                            + key.toString().split(Config.DELIMER)[0]
                            + "\t" + String.valueOf(url_id)),
                    new Text(String.valueOf(mean)));
        }
    }
}