import nltk
from nltk.stem.snowball import SnowballStemmer
from nltk import word_tokenize
from nltk.corpus import stopwords

letters = "0123456789qwertyuiopasdfghjklzxcvbnmйцукенгшщзхъфывапролджэячсмитьбюё"

stopwords_english = list(set(stopwords.words('english')))
stopwords_russian = list(set(stopwords.words('russian')))

for i, w in enumerate(stopwords_english):
    new_word = ""
    for ww in w:
        if ww.isalpha():
            new_word += ww

english_stemmer = SnowballStemmer("english")
russian_stemmer = SnowballStemmer("russian")


def _transform_word(word):
    word = word.strip()

    if len(word) == 0:
        return ""

    flag = True
    for w in word:
        if w in letters:
            flag = False
            break

    if flag:
        return ""

    flag = True
    for w in word:
        if w.isdigit() or w == "." or w == "-" or w == "(" or w == ")" or w == "%":
            continue
        else:
            flag = False
            break

    if flag:
        new_word = ""
        for w in word:
            if w.isdigit():
                new_word += w
        return new_word

    word = word.replace("ё", "е")

    # для 1с
    if word == "1с" or word == "1c":
        word = word.replace("с", "c")

    tmp_word = ""
    for i, w in enumerate(word):
        if w.isdigit() or w.isalpha():
            tmp_word += w
    word = tmp_word

    if word in stopwords_russian or word in stopwords_english:
        return ""

    word = english_stemmer.stem(word)
    word = russian_stemmer.stem(word)

    return word


def my_word_tokenize(text):
    if len(text) > 0:
        words_space = text.split(" ")
        words_comma = list()
        for word in words_space:
            for word2 in word.split(","):
                if len(word2) > 0:
                    words_comma.append(word2)

        words = list(map(_transform_word, words_comma))
        words = list(filter(lambda x: len(x) > 0, words))
        word = " ".join(words)
        word = word.strip()
        return word
    else:
        return ""


DIR = "/home/boris/PycharmProjects/finalProject/data/queries/"

queries_csv = open(DIR + "correct_queries_1.csv", "r")
out_csv = open(DIR + "stem_correct_queries_1.csv", "w")

for num_line, line in enumerate(queries_csv):
    if num_line % 100 == 0:
        print(num_line)

    line = line.lower()
    parts = line.split("\t")
    num = parts[0]
    query = parts[1]

    tokenize_query = my_word_tokenize(query)

    if len(tokenize_query) != 0:
        out_csv.write(num + "\t" + tokenize_query + "\n")
    else:
        out_csv.write(num + "\t" + query)

queries_csv.close()
out_csv.close()
