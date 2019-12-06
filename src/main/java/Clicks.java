import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

public class Clicks {

    private String type = "QueryUrl";
    private HashSet<String> trainSet, testSet;
    private HashMap<String, Integer> urlId, queryId;

    Clicks(HashSet<String> trainSet,
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
        for(String clickedUrl: serp.clicked){
            if(trainSet.contains(clickedUrl) || testSet.contains(clickedUrl)) {
                if(this.type.equals("QueryUrl")) {
                    if(queryId.containsKey(serp.query) && urlId.containsKey(clickedUrl)) {
                        context.write(new Text(Config.CLICK + Config.DELIMER + queryId.get(serp.query) + Config.DELIMER + urlId.get(clickedUrl)), new Text("1"));
                    }
                } else{
                    if(urlId.containsKey(clickedUrl)) {
                        context.write(new Text(Config.CLICK + Config.DELIMER + urlId.get(clickedUrl)), new Text("1"));
                    }
                }
            }
        }
    }

    public void reduce(Text key,
                       Iterable<Text> values,
                       Reducer.Context context) throws IOException, InterruptedException {

        double count = 0;
        for(Text value : values){
            count = count + Double.valueOf(value.toString());
        }

        if(this.type.equals("QueryUrl")) {
            int url_id = Integer.valueOf(key.toString().split(Config.DELIMER)[2]);
            int query_id = Integer.valueOf(key.toString().split(Config.DELIMER)[1]);
            context.write(new Text("QueryUrl\t"
                            + key.toString().split(Config.DELIMER)[0] + "\t"
                            + String.valueOf(query_id) + "\t"
                            + String.valueOf(url_id)),
                    new Text(String.valueOf(count)));
        } else{
            int url_id = Integer.valueOf(key.toString().split(Config.DELIMER)[1]);
            context.write(new Text("Url\t"
                            + key.toString().split(Config.DELIMER)[0] + "\t"
                            + String.valueOf(url_id)),
                    new Text(String.valueOf(count)));
        }
    }
}
