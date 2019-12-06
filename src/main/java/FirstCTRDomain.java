import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

public class FirstCTRDomain {

    private HashSet<String> trainDomainSet, testDomainSet;
    private HashMap<String, Integer> domainId;

    FirstCTRDomain(HashSet<String> trainDomainSet,
              HashSet<String> testDomainSet,
              HashMap<String, Integer> domainId) {
        this.trainDomainSet = trainDomainSet;
        this.testDomainSet = testDomainSet;
        this.domainId = domainId;
    }

    public void map(Mapper.Context context, SERP serp) throws IOException, InterruptedException {
        int idx;
        String firstUrl = serp.clicked.get(0);
        String firstDomain = firstUrl.split("/")[0];
        if (trainDomainSet.contains(firstDomain) || testDomainSet.contains(firstDomain)) {

            for (String url : serp.urls) {
                String domain = url.split("/")[0];
                if (trainDomainSet.contains(domain) || testDomainSet.contains(domain)) {

                    if (firstUrl.equals(url)) {
                        idx = 1;
                    } else {
                        idx = 0;
                    }

                    if(domainId.containsKey(domain)) {
                            context.write(new Text(Config.FIRST_CTR_DOMAIN + Config.DELIMER + domainId.get(domain)), new Text(String.valueOf(idx)));
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
            click = click + Double.valueOf(isClicked.toString());
        }

        int domain_id = Integer.valueOf(key.toString().split(Config.DELIMER)[1]);
        context.write(new Text("Domain\t"
                        + key.toString().split(Config.DELIMER)[0] + "\t"
                        + String.valueOf(domain_id)),
                new Text(String.valueOf(click / show)));
    }
}
