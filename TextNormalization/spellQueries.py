from json import JSONDecodeError

import requests
import pickle

# DIR = "data/queries/"
URL = "https://speller.yandex.net/services/spellservice.json/checkText"

queries = open("queries.txt", "r")
# correct_queries = open("correct_queries.txt", "w")
dict_ = dict()

for num, line in enumerate(queries):
    # parts = line.split("\t")
    # num = parts[0]
    query = line[:-1]

    while True:
        try:
            data = requests.get(URL, params={'text': query})
            json_data = data.json()
        except JSONDecodeError:
            print("EXCEPTION: " + str(num))
            continue
        break

    if len(json_data) > 0:
        correct_query = ""
        curr_pos = 0
        for correct_word in json_data:
            position = int(correct_word['pos'])
            length = int(correct_word['len'])
            correct_query = correct_query + query[curr_pos:position] + correct_word['s'][0]
            curr_pos = position + length

        if len(query[curr_pos:]) > 0:
            correct_query = correct_query + query[curr_pos:-1]

        dict_[query] = correct_query
        # correct_queries.write(str(num) + "\t" + str(correct_query) + "\n")
    else:
        dict_[query] = query
    # correct_query = query
    # correct_queries.write(str(num) + "\t" + str(correct_query))

    print(num)

with open("correct_queries.pickle", "wb") as f:
    pickle.dump(dict_, f)

print(dict_)

# correct_queries.close()
