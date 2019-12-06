import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

public class ProbClickDownDQ {

    private HashSet<String> trainDomainSet, testDomainSet;
    private HashMap<String, Integer> domainId, queryId;

    ProbClickDownDQ(HashSet<String> trainDomainSet,
              HashSet<String> testDomainSet,
              HashMap<String, Integer> domainId,
              HashMap<String, Integer> queryId) {
        this.trainDomainSet = trainDomainSet;
        this.testDomainSet = testDomainSet;
        this.domainId = domainId;
        this.queryId = queryId;
    }

    public void map(Mapper.Context context, SERP serp) throws IOException, InterruptedException {
        int idx = 0;

        for (String link : serp.urls) {
            if(idx == serp.urls.size()-1){
                break;
            }
            String domain = link.split("/")[0];

            if(trainDomainSet.contains(domain) || testDomainSet.contains(domain)) {
                String downUrl = serp.urls.get(idx+1);
                if (serp.clicked.contains(downUrl)) {
                    if(queryId.containsKey(serp.query) && domainId.containsKey(domain)) {
                        context.write(new Text(Config.PROB_DOWN_DQ + Config.DELIMER + domainId.get(domain) + Config.DELIMER + queryId.get(serp.query)),
                                new Text("1"));
                    }
                } else {
                    if(queryId.containsKey(serp.query) && domainId.containsKey(domain)) {
                        context.write(new Text(Config.PROB_DOWN_DQ + Config.DELIMER + domainId.get(domain) + Config.DELIMER + queryId.get(serp.query)),
                                new Text("0"));
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

        int domain_id = Integer.valueOf(key.toString().split(Config.DELIMER)[1]);
        int query_id = Integer.valueOf(key.toString().split(Config.DELIMER)[2]);
        context.write(new Text("DomainQuery\t"
                        + key.toString().split(Config.DELIMER)[0] + "\t"
                        + String.valueOf(domain_id) + "\t"
                        + String.valueOf(query_id)),
                new Text(String.valueOf(prob)));
    }
}
