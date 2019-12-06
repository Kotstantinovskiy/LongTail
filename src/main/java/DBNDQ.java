import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

public class DBNDQ {
    private HashSet<String> trainDomainSet, testDomainSet;
    private HashMap<String, Integer> domainId, queryId;

    DBNDQ(HashSet<String> trainDomainSet,
                    HashSet<String> testDomainSet,
                    HashMap<String, Integer> domainId,
                    HashMap<String, Integer> queryId) {
        this.trainDomainSet = trainDomainSet;
        this.testDomainSet = testDomainSet;
        this.domainId = domainId;
        this.queryId = queryId;
    }

    public void map(Mapper.Context context, SERP serp) throws IOException, InterruptedException {

        String lastClickedUrl = serp.clicked.get(serp.clicked.size() - 1);

        HashMap<String, Long> a_D = new HashMap<>();
        HashMap<String, Long> a_N = new HashMap<>();
        HashMap<String, Long> s_D = new HashMap<>();
        HashMap<String, Long> s_N = new HashMap<>();

        for(String url : serp.urls){
            if (url.equals(lastClickedUrl)) {
                a_D.put(url, (long) 1);
                break;
            } else {
                a_D.put(url, (long) 1);
            }

        }

        for (String clickedUrl : serp.clicked) {
            a_N.put(clickedUrl, (long) 1);
            s_D.put(clickedUrl, (long) 1);

        }

        s_N.put(lastClickedUrl, (long) 1);

        for (String link : serp.urls) {
            String domain = link.split("/")[0];
            if(trainDomainSet.contains(domain) || testDomainSet.contains(domain)) {
                if(queryId.containsKey(serp.query) && domainId.containsKey(domain)) {
                    context.write(new Text(Config.DBN_DQ + Config.DELIMER + domainId.get(domain) + Config.DELIMER + queryId.get(serp.query)),
                            new Text(Double.toString(a_N.getOrDefault(domain, (long) 0)) +
                                    Config.DELIMER + Double.toString(a_D.getOrDefault(domain, (long) 0)) +
                                    Config.DELIMER + Double.toString(s_N.getOrDefault(domain, (long) 0)) +
                                    Config.DELIMER + Double.toString(s_D.getOrDefault(domain, (long) 0))));
                }
            }
        }
    }

    public void reduce(Text key,
                       Iterable<Text> values,
                       Reducer.Context context) throws IOException, InterruptedException {

        double a_D = 0.0;
        double a_N = 0.0;
        double s_D = 0.0;
        double s_N = 0.0;

        final double alpha1 = 0.2;
        final double beta1 = 0.2;
        final double alpha2 = 0.2;
        final double beta2 = 0.2;

        for (Text value : values) {
            String[] parts = value.toString().split(Config.DELIMER);
            a_N += Double.parseDouble(parts[0]);
            a_D += Double.parseDouble(parts[1]);
            s_N += Double.parseDouble(parts[2]);
            s_D += Double.parseDouble(parts[3]);
        }

        int query_id = Integer.valueOf(key.toString().split(Config.DELIMER)[2]);
        int domain_id = Integer.valueOf(key.toString().split(Config.DELIMER)[1]);
        context.write(new Text("DomainQuery\t"
                        + key.toString().split(Config.DELIMER)[0] + "\t"
                        + String.valueOf(domain_id) + "\t"
                        + String.valueOf(query_id)),
                new Text(String.valueOf((a_N + alpha1) / (a_D + alpha1 + beta1)) + "\t"
                        + String.valueOf((s_N + alpha2) / (s_D + alpha2 + beta2))));
    }
}
