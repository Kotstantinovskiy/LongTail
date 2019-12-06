import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

public class MeanPositionClickDomain {

    private HashSet<String> trainDomainSet, testDomainSet;
    private HashMap<String, Integer> domainId;

    MeanPositionClickDomain(HashSet<String> trainDomainSet,
                  HashSet<String> testDomainSet,
                  HashMap<String, Integer> domainId) {
        this.trainDomainSet = trainDomainSet;
        this.testDomainSet = testDomainSet;
        this.domainId = domainId;
    }

    public void map(Mapper.Context context, SERP serp) throws IOException, InterruptedException {
        for(String url : serp.urls){
            String domain = url.split("/")[0];
            if (trainDomainSet.contains(domain) || testDomainSet.contains(domain)) {
                int idx = serp.clicked.indexOf(url);
                if (idx != -1) {
                    if(domainId.containsKey(domain)) {
                        context.write(new Text(Config.MEAN_POSITION_CLICK_DOMAIN + Config.DELIMER + domainId.get(domain)),
                                new Text(String.valueOf(idx)));
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
            mean = mean + Double.valueOf(value.toString());
        }

        mean = mean / length;

        int domain_id = Integer.valueOf(key.toString().split(Config.DELIMER)[1]);
        context.write(new Text("Domain\t"
                        + key.toString().split(Config.DELIMER)[0] + "\t"
                        + String.valueOf(domain_id)),
                new Text(String.valueOf(mean)));
    }
}
