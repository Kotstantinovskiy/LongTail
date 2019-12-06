import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;

public class CTRDomain {
    private HashSet<String> trainDomainSet, testDomainSet;
    private HashMap<String, Integer> domainId;

    CTRDomain(HashSet<String> trainDomainSet,
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

                if(domainId.containsKey(domain)) {
                    context.write(new Text(Config.CTR_DOMAIN_1 + Config.DELIMER + domainId.get(domain)),
                            new Text(String.valueOf(countClick) + " " + String.valueOf(countAll)));
                    context.write(new Text(Config.CTR_DOMAIN_2 + Config.DELIMER + domainId.get(domain)),
                            new Text(String.valueOf(countClick / countAll)));
                }
            }
        }
    }

    public void reduce(Text key,
                       Iterable<Text> values,
                       Reducer.Context context) throws IOException, InterruptedException {
        if(key.toString().split(Config.DELIMER)[0].equals(Config.CTR_DOMAIN_1)) {
            double clickAll = 0, showAll = 0;
            for (Text value : values) {
                clickAll += Double.valueOf(value.toString().split(" ")[0]);
                showAll += Double.valueOf(value.toString().split(" ")[1]);
            }

            int url_id = Integer.valueOf(key.toString().split(Config.DELIMER)[1]);
            context.write(new Text("Domain\t"
                            + key.toString().split(Config.DELIMER)[0] + "\t"
                            + String.valueOf(url_id)),
                    new Text(String.valueOf(clickAll/showAll)));
        } else if(key.toString().split(Config.DELIMER)[0].equals(Config.CTR_DOMAIN_2)){
            double n = 0, allCTR = 0;
            for (Text value : values) {
                n++;
                allCTR += Double.valueOf(value.toString());
            }

            int url_id = Integer.valueOf(key.toString().split(Config.DELIMER)[1]);
            context.write(new Text("Domain\t"
                            + key.toString().split(Config.DELIMER)[0] + "\t"
                            + String.valueOf(url_id)),
                    new Text(String.valueOf(allCTR/n)));

        }
    }
}
