import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

public class ImpDomain {
    private HashSet<String> trainDomainSet, testDomainSet;
    private HashMap<String, Integer> domainId;

    ImpDomain(HashSet<String> trainDomainSet,
              HashSet<String> testDomainSet,
              HashMap<String, Integer> domainId) {
        this.trainDomainSet = trainDomainSet;
        this.testDomainSet = testDomainSet;
        this.domainId = domainId;
    }

    public void map(Mapper.Context context, SERP serp) throws IOException, InterruptedException {
        HashSet<String> urlsSet = new HashSet<>(serp.urls);

        for(String url : urlsSet) {
            String domain = url.split("/")[0];
            if (trainDomainSet.contains(domain) || testDomainSet.contains(domain)) {
                double count = 0;

                for(String allDomain: serp.urls) {
                    if(allDomain.split("/")[0].equals(domain)){
                        count = count + 1;
                    }
                }

                if(domainId.containsKey(domain)) {
                    context.write(new Text(Config.IMP_DOMAIN + Config.DELIMER + domainId.get(domain)),
                            new Text(String.valueOf(count)));
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

        double domain_id = Integer.valueOf(key.toString().split(Config.DELIMER)[1]);
        context.write(new Text("Domain\t"
                        + key.toString().split(Config.DELIMER)[0] + "\t"
                        + String.valueOf(domain_id)),
                new Text(String.valueOf(count)));
    }
}
