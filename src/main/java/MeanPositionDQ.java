import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

public class MeanPositionDQ {

    private HashSet<String> trainDomainSet, testDomainSet;
    private HashMap<String, Integer> domainId, queryId;

    MeanPositionDQ(HashSet<String> trainDomainSet,
                   HashSet<String> testDomainSet,
                   HashMap<String, Integer> domainId,
                   HashMap<String, Integer> queryId) {
        this.trainDomainSet = trainDomainSet;
        this.testDomainSet = testDomainSet;
        this.domainId = domainId;
        this.queryId = queryId;
    }

    public void map(SERP serp, Mapper.Context context) throws IOException, InterruptedException {
        int idx = 0;
        for(String url : serp.urls){
            String domain = url.split("/")[0];
            if(trainDomainSet.contains(domain) || testDomainSet.contains(domain)) {
                if(queryId.containsKey(serp.query) && domainId.containsKey(domain)) {
                    context.write(new Text(Config.MEAN_POSITION_DQ + Config.DELIMER + domainId.get(domain) + Config.DELIMER + queryId.get(serp.query)),
                            new Text(String.valueOf(idx)));
                }
            }
            idx++;
        }
    }

    public void reduce(Text key,
                       Iterable<Text> values,
                       Reducer.Context context) throws IOException, InterruptedException {
        double mean = 0, length = 0;

        for(Text value : values){
            length++;
            mean = mean + Double.valueOf(value.toString());
        }

        mean = mean / length;

        int domain_id = Integer.valueOf(key.toString().split(Config.DELIMER)[1]);
        int query_id = Integer.valueOf(key.toString().split(Config.DELIMER)[2]);
        context.write(new Text("DomainQuery\t"
                        + key.toString().split(Config.DELIMER)[0] + "\t"
                        + String.valueOf(domain_id) + "\t"
                        + String.valueOf(query_id)),
                new Text(String.valueOf(mean)));
    }
}
