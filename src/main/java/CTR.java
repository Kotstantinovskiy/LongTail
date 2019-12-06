import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

public class CTR {

    private String type = "QueryUrl";
    private HashSet<String> trainSet;
    private HashSet<String> testSet;
    private HashMap<String, Integer> urlId, queryId;

    CTR(HashSet<String> trainSet,
        HashSet<String> testSet,
        HashMap<String, Integer> urlId,
        HashMap<String, Integer> queryId,
        String type){
        if(type.equals("Url")){
            this.type = type;
        }
        this.trainSet = trainSet;
        this.testSet = testSet;
        this.urlId= urlId;
        this.queryId = queryId;
    }

    public void map(Mapper.Context context, SERP serp) throws IOException, InterruptedException {
        for(String url : serp.urls) {
            if (trainSet.contains(url) || testSet.contains(url)) {
                int idx = serp.clicked.indexOf(url);

                if (idx == -1) {
                    idx = 0;
                } else {
                    idx = 1;
                }

                if (type.equals("QueryUrl")) {
                    if(queryId.containsKey(serp.query) && urlId.containsKey(url)) {
                        context.write(new Text(Config.CTR + Config.DELIMER + queryId.get(serp.query) + Config.DELIMER + urlId.get(url)),
                                new Text(String.valueOf(idx)));
                    }
                } else {
                    if(urlId.containsKey(url)) {
                        context.write(new Text(Config.CTR + Config.DELIMER + urlId.get(url)),
                                new Text(String.valueOf(idx)));
                    }
                }
            }
        }
    }

    public void reduce(Text key,
                       Iterable<Text> clicks,
                       Reducer.Context context) throws IOException, InterruptedException {
        double show = 0, click = 0;
        for(Text isClicked : clicks){
            show++;
            click =+ Double.valueOf(isClicked.toString());
        }


        if(this.type.equals("QueryUrl")) {
            int query_id = Integer.valueOf(key.toString().split(Config.DELIMER)[1]);
            int url_id = Integer.valueOf(key.toString().split(Config.DELIMER)[2]);
            context.write(new Text("QueryUrl\t"
                            + key.toString().split(Config.DELIMER)[0] + "\t"
                            + String.valueOf(query_id) + "\t"
                            + String.valueOf(url_id)),
                    new Text(String.valueOf(click / show)));
        } else {
            int url_id = Integer.valueOf(key.toString().split(Config.DELIMER)[1]);
            context.write(new Text("Url\t"
                            + key.toString().split(Config.DELIMER)[0] + "\t"
                            + String.valueOf(url_id)),
                    new Text(String.valueOf(click / show)));
        }
    }
}
