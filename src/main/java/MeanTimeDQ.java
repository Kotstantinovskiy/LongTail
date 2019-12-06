import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class MeanTimeDQ {

    private HashSet<String> trainDomainSet, testDomainSet;
    private HashMap<String, Integer> domainId, queryId;

    MeanTimeDQ(HashSet<String> trainDomainSet,
                   HashSet<String> testDomainSet,
                   HashMap<String, Integer> domainId,
                   HashMap<String, Integer> queryId) {
        this.trainDomainSet = trainDomainSet;
        this.testDomainSet = testDomainSet;
        this.domainId = domainId;
        this.queryId = queryId;
    }

    public void map(Mapper.Context context, SERP serp) throws IOException, InterruptedException {
        if(serp.timestamps.size() >= 2) {
            for (int i = 0; i < serp.timestamps.size() - 1; i++) {
                long delta_time = Math.abs(serp.timestamps.get(i) - serp.timestamps.get(i + 1));
                String domain = serp.clicked.get(i).split("/")[0];
                if(trainDomainSet.contains(domain) || testDomainSet.contains(domain)) {
                    if(queryId.containsKey(serp.query) && domainId.containsKey(domain)) {
                        context.write(new Text(Config.MEAN_TIME_DQ + Config.DELIMER + domainId.get(domain) + Config.DELIMER + queryId.get(serp.query)),
                                new Text(String.valueOf(delta_time)));
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

        int query_id = Integer.valueOf(key.toString().split(Config.DELIMER)[2]);
        int domain_id = Integer.valueOf(key.toString().split(Config.DELIMER)[1]);
        context.write(new Text("DomainQuery\t"
                        + key.toString().split(Config.DELIMER)[0] + "\t"
                        + String.valueOf(domain_id) + "\t"
                        + String.valueOf(query_id)),
                new Text(String.valueOf(mean)));
    }
}