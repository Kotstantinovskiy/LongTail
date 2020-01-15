public class Config {
    public static final String QUERIES = "/user/b.konstantinovskij/final/data/correct_queries_norm.tsv";
    public static final String QUERIES_SET = "/user/b.konstantinovskij/final/data/queries_set.txt";
    public static final String DOC_QUERIES = "/user/b.konstantinovskij/final/data/doc_queries.txt";
    //public static final String PATH_TMP = "/user/b.konstantinovskij/final/data/tmp.txt";

    public static final String DOCS_ALL_IDF = "/user/b.konstantinovskij/final/df/idf_all.txt";
    public static final String DOCS_ALL_ICF = "/user/b.konstantinovskij/final/cf/icf_all.txt";
    public static final String DOCS_ALL_IDF_BI = "/user/b.konstantinovskij/final/df/idf_all_bi.txt";
    public static final String DOCS_ALL_ICF_BI = "/user/b.konstantinovskij/final/cf/icf_all_bi.txt";

    public static final String DOCS_TITLE_IDF = "/user/b.konstantinovskij/final/df/idf_title.txt";
    public static final String DOCS_TITLE_ICF = "/user/b.konstantinovskij/final/cf/icf_title.txt";
    public static final String DOCS_TITLE_IDF_BI = "/user/b.konstantinovskij/final/df/idf_title_bi.txt";
    public static final String DOCS_TITLE_ICF_BI = "/user/b.konstantinovskij/final/cf/icf_title_bi.txt";

    public static final String DOCS_TEXT_IDF = "/user/b.konstantinovskij/final/df/idf_text.txt";
    public static final String DOCS_TEXT_ICF = "/user/b.konstantinovskij/final/cf/icf_text.txt";
    public static final String DOCS_TEXT_IDF_BI = "/user/b.konstantinovskij/final/df/idf_text_bi.txt";
    public static final String DOCS_TEXT_ICF_BI = "/user/b.konstantinovskij/final/cf/icf_text_bi.txt";

    //public static final String QUERIES = "/home/boris/Рабочий стол/Техносфера/Hadoop/FinalCompetition/data/stem_correct_queries.csv";
    //public static final String DOC_QUERIES = "/home/boris/Рабочий стол/Техносфера/Hadoop/FinalCompetition/data/doc_queries.txt";
    //public static final String PATH_TMP = "/home/boris/Рабочий стол/Техносфера/Hadoop/FinalCompetition/data/tmp.txt";

    public static final double k1 = 2.0;
    public static final double b = 0.75;
    public static final double MIN = 0.0;
    public static final double LEN_ALL = 3778740680.0;
    public static final double LEN_TITLE = 3922866.0;
    public static final double LEN_TEXT = 3774817814.0;
    public static final double N = 582167;

    static final public String DELIMER = "%:!%";
    static final public String CTR = "CTR";
    static final public String FIRST_CTR = "FIRST_CTR";
    static final public String LAST_CTR = "LAST_CTR";
    static final public String ONLY_CTR = "ONLY_CTR";
    static final public String DBN = "DBN";
    static final public String PROB_LAST = "PROB_LAST";
    static final public String MEAN_TIME = "MEAN_TIME";
    static final public String MEAN_POSITION = "MEAN_POSITION";
    static final public String MEAN_NUM_CLICK = "MEAN_NUM_CLICK";
    static final public String PROB_UPPER = "PROB_UPPER";
    static final public String PROB_DOWN = "PROB_DOWN";
    static final public String MEAN_POSITION_CLICK = "MEAN_POSITION_CLICK";
    static final public String MEAN_COUNT_CLICK_UP = "MEAN_COUNT_CLICK_UP";
    static final public String IMP = "IMP";
    static final public String CLICK = "CLICK";

    static final public String CTR_DOMAIN_1 = "CTR_DOMAIN_1";
    static final public String CTR_DOMAIN_2 = "CTR_DOMAIN_2";
    static final public String IMP_DOMAIN = "IMP_DOMAIN";
    static final public String CLICK_DOMAIN = "CLICK_DOMAIN";
    static final public String FIRST_CTR_DOMAIN = "FIRST_CTR_DOMAIN";
    static final public String LAST_CTR_DOMAIN = "LAST_CTR_DOMAIN";
    static final public String MEAN_POSITION_CLICK_DOMAIN = "MEAN_POSITION_CLICK_DOMAIN";
    static final public String MEAN_TIME_DOMAIN = "MEAN_TIME_DOMAIN";
    static final public String ONLY_CTR_DOMAIN = "ONLY_CTR_DOMAIN";

    static final public String CLICK_DQ = "CLICK_DQ";
    static final public String CTR_DQ = "CTR_DQ";
    static final public String DBN_DQ = "DBN_DQ";
    static final public String FIRST_CTR_DQ = "FIRST_CTR_DQ";
    static final public String IMP_DQ = "IMP_DQ";
    static final public String LAST_CTR_DQ = "LAST_CTR_DQ";
    static final public String MEAN_COUNT_CLICK_UP_DQ = "MEAN_COUNT_CLICK_UP_DQ";
    static final public String MEAN_NUM_CLICK_DQ = "MEAN_NUM_CLICK_DQ";
    static final public String MEAN_POSITION_DQ = "MEAN_POSITION_DQ";
    static final public String MEAN_POSITION_CLICK_DQ = "MEAN_POSITION_CLICK_DQ";
    static final public String MEAN_TIME_DQ = "MEAN_TIME_DQ";
    static final public String ONLY_CTR_DQ = "ONLY_CTR_DQ";
    static final public String PROB_DOWN_DQ = "PROB_DOWN_DQ";
    static final public String PROB_UPPER_DQ = "PROB_UPPER_DQ";
    static final public String PROB_LAST_DQ = "PROB_LAST_DQ";

    static final public String PATH_TRAIN = "/user/b.konstantinovskij/user_behaviour/train_urls.txt";
    static final public String PATH_TEST = "/user/b.konstantinovskij/user_behaviour/test_urls.txt";
    static final public String PATH_DOMAIN_TRAIN = "/user/b.konstantinovskij/user_behaviour/domains_train.txt";
    static final public String PATH_DOMAIN_TEST = "/user/b.konstantinovskij/user_behaviour/domains_test.txt";
    static final public String PATH_ID_QUERY = "/user/b.konstantinovskij/user_behaviour/queries.tsv";
    static final public String PATH_ID_URL = "/user/b.konstantinovskij/user_behaviour/urls.csv";
    static final public String PATH_ID_DOMAIN = "/user/b.konstantinovskij/user_behaviour/domains_id.tsv";

    /*
    static final public String PATH_TRAIN = "/home/boris/Рабочий стол/Техносфера/Hadoop/FinalCompetition/data/Train/train_urls.txt";
    static final public String PATH_TEST = "/home/boris/Рабочий стол/Техносфера/Hadoop/FinalCompetition/data/Test/test_urls.txt";
    static final public String PATH_DOMAIN_TRAIN = "/home/boris/Рабочий стол/Техносфера/Hadoop/FinalCompetition/data/Train/domains_train.txt";
    static final public String PATH_DOMAIN_TEST = "/home/boris/Рабочий стол/Техносфера/Hadoop/FinalCompetition/data/Test/domains_test.txt";
    static final public String PATH_ID_QUERY = "/home/boris/Рабочий стол/Техносфера/Hadoop/FinalCompetition/data/queries.tsv";
    static final public String PATH_ID_URL = "/home/boris/Рабочий стол/Техносфера/Hadoop/FinalCompetition/data/urls.csv";
    static final public String PATH_ID_DOMAIN = "/home/boris/Рабочий стол/Техносфера/Hadoop/FinalCompetition/data/domains_id.tsv";
    */
    public static final int REDUCE_COUNT = 10;
}
