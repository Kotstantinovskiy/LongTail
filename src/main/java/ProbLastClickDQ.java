import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

public class ProbLastClickDQ {

    private HashSet<String> trainDomainSet, testDomainSet;
    private HashMap<String, Integer> domainId, queryId;

    ProbLastClickDQ(HashSet<String> trainDomainSet,
                  HashSet<String> testDomainSet,
                  HashMap<String, Integer> domainId,
                  HashMap<String, Integer> queryId) {
        this.trainDomainSet = trainDomainSet;
        this.testDomainSet = testDomainSet;
        this.domainId = domainId;
        this.queryId = queryId;
    }

    public void map(Mapper.Context context, SERP serp) throws IOException, InterruptedException {
        String lastClickDomain = serp.clicked.get(serp.clicked.size() - 1).split("/")[0];

        if(trainDomainSet.contains(lastClickDomain) || testDomainSet.contains(lastClickDomain)) {
            for (String link : serp.urls) {
                String domain = link.split("/")[0];
                if (trainDomainSet.contains(domain) || testDomainSet.contains(domain)) {
                    if (lastClickDomain.equals(domain)) {
                        if(queryId.containsKey(serp.query) && domainId.containsKey(domain)) {
                            context.write(new Text(Config.PROB_LAST_DQ + Config.DELIMER + domainId.get(domain) + Config.DELIMER + queryId.get(serp.query)),
                                    new Text("1"));
                        }
                    } else {
                        if(queryId.containsKey(serp.query) && domainId.containsKey(domain)) {
                            context.write(new Text(Config.PROB_LAST_DQ + Config.DELIMER + domainId.get(domain) + Config.DELIMER + queryId.get(serp.query)),
                                    new Text("0"));
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

        int query_id = Integer.valueOf(key.toString().split(Config.DELIMER)[2]);
        int domain_id = Integer.valueOf(key.toString().split(Config.DELIMER)[1]);
        context.write(new Text("DomainQuery\t"
                        + key.toString().split(Config.DELIMER)[0] + "\t"
                        + String.valueOf(domain_id) + "\t"
                        + String.valueOf(query_id)),
                new Text(String.valueOf(prob)));
    }
}
