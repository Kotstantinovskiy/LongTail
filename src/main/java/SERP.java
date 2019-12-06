import java.util.ArrayList;
import java.util.Arrays;

class SERP {

    String query;
    long region;
    ArrayList<String> urls;
    ArrayList<String> clicked = new ArrayList<>();
    ArrayList<Long> timestamps = new ArrayList<>();

    public SERP(String row){
        //this.row = row;

        String[] parts = row.trim().split("\t");
        this.query = parts[0].split("@")[0];
        this.region = Long.parseLong(parts[0].split("@")[1]);

        this.urls = new ArrayList<String>(Arrays.asList(parts[1].trim()
                .replace("http://", "")
                .replace("https://", "")
                .replace("www.", "")
                .split(",")));

        String[] clicked_urls = parts[2].trim()
                .replace("http://", "")
                .replace("https://", "")
                .replace("www.", "")
                .split(",");

        for(String click_url : clicked_urls){
            for(String url : urls){
                if(url.equals(click_url)){
                    this.clicked.add(url);
                    break;
                }
            }
        }

        String[] timestampArray = parts[3].split(",");
        for(String timestamp : timestampArray){
            this.timestamps.add(Long.valueOf(timestamp));
        }
    }
}
