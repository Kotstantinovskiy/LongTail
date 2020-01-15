from json import JSONDecodeError
import tldextract
import requests
import pickle

API_KEY = "ogk8c0k40cko840okkgo8kc48ksc4sc8o408k84s"
URL = "https://openpagerank.com/api/v1.0/getPageRank"
DIR = "/home/boris/PycharmProjects/finalProject/data/urls/"

file_url = open(DIR + "url.tsv", "r")
#domains = set()
header = {"API-OPR": API_KEY}
'''
for j, line in enumerate(file_url):
    print(j)
    num = int(line.split("\t")[0])
    url = line.split("\t")[1]

    parse_url = tldextract.extract(url)
    domains.add(parse_url.domain + "." + parse_url.suffix)

domains = list(domains)
with open(DIR + 'domains.pickle', 'wb') as f:
    pickle.dump(domains, f)
'''

with open(DIR + 'domains.pickle', 'rb') as f:
    domains = pickle.load(f)
print(len(domains))
domains_list = []
result = {}
for i, domain in enumerate(domains[100800:]):
    domains_list.append(domain)

while True:
    try:
        params = {"domains[]": domains_list}
        data = requests.get(URL, params=params, headers=header)
        json_data = data.json()
        for page_rank_domain in json_data["response"]:
            if int(page_rank_domain["status_code"]) == 200:
                result[page_rank_domain["domain"]] = float(page_rank_domain["page_rank_decimal"])
            else:
                result[page_rank_domain["domain"]] = -1
    except JSONDecodeError:
        print("EXCEPTION!")
        continue
    break

out_file = open(DIR + "page_rank_2.txt", "w")
for i in result.keys():
    out_file.write(i + "\t" + str(result[i]) + "\n")
out_file.close()