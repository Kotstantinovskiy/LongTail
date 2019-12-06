import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;

public class CTRDQ {
    private HashSet<String> trainDomainSet, testDomainSet;
    private HashMap<String, Integer> domainId, queryId;

    CTRDQ(HashSet<String> trainDomainSet,
              HashSet<String> testDomainSet,
              HashMap<String, Integer> domainId,
          HashMap<String, Integer> queryId) {
        this.trainDomainSet = trainDomainSet;
        this.testDomainSet = testDomainSet;
        this.domainId = domainId;
        this.queryId = queryId;
    }

    public void map(Mapper.Context context, SERP serp) throws IOException, InterruptedException {
        HashSet<String> urlsSet = new HashSet<>(serp.urls);

        for(String url : urlsSet) {
            String domain = url.split("/")[0];
            if (trainDomainSet.contains(domain) || testDomainSet.contains(domain)) {
                int countAll = 0, countClick = 0;

                for(String allDomain: serp.urls) {
                    if(allDomain.split("/")[0].equals(domain)){
                        countAll += 1;
                    }
                }

                for(String clickDomain: serp.clicked) {
                    if(clickDomain.split("/")[0].equals(domain)){
                        countClick += 1;
                    }
                }

                if(domainId.containsKey(domain) && queryId.containsKey(serp.query)) {
                    context.write(new Text(Config.CTR_DQ + Config.DELIMER + domainId.get(domain) + Config.DELIMER + queryId.get(serp.query)),
                            new Text(String.valueOf(countClick) + " " + String.valueOf(countAll)));
                }
            }
        }
    }

    public void reduce(Text key,
                       Iterable<Text> values,
                       Reducer.Context context) throws IOException, InterruptedException {

        double clickAll = 0, showAll = 0;
        for (Text value : values) {
            clickAll += Double.valueOf(value.toString().split(" ")[0]);
            showAll += Double.valueOf(value.toString().split(" ")[1]);
        }

        int url_id = Integer.valueOf(key.toString().split(Config.DELIMER)[1]);
        int query_id = Integer.valueOf(key.toString().split(Config.DELIMER)[2]);
        context.write(new Text("DomainQuery\t"
                        + key.toString().split(Config.DELIMER)[0] + "\t"
                        + String.valueOf(url_id) + "\t"
                        + String.valueOf(query_id)),
                new Text(String.valueOf(clickAll/showAll)));
    }
}
