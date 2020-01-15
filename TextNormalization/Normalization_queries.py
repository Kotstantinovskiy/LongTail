from spacy.lang.ru import Russian
nlp = Russian()

from spacy_russian_tokenizer import RussianTokenizer, MERGE_PATTERNS
russian_tokenizer = RussianTokenizer(nlp, MERGE_PATTERNS)
nlp.add_pipe(russian_tokenizer, name='russian_tokenizer')

from pymystem3 import Mystem
my_stem = Mystem()

from nltk.stem.snowball import SnowballStemmer
stemmer = SnowballStemmer("english")

from stop_words import get_stop_words
stopwords_english = get_stop_words('en')
stopwords_russian = get_stop_words('ru')

def filter_symbols(word):
	return ''.join(list(filter(lambda x: x.isalpha() or x.isdigit(), word)))

queries_in = open("correct_queries.csv", "r")
queries_out = open("correct_queries_norm.tsv", "w")

for i, line in enumerate(queries_in):	
	splits = line.split("\t")
    
	num = int(splits[0])
	query = str.lower(splits[1])

	query_tokens = nlp(query)
    
	new_query = []
	for token in query_tokens:
		if (token.text in stopwords_english) or (token.text in stopwords_russian):
			continue
	
		lemmas = my_stem.lemmatize(token.text)
		word = ''.join(list(filter(lambda x: len(x) != 0, list(map(filter_symbols, lemmas)))))
		stemmer.stem(word)
		new_query.append(word)
    
	queries_out.write(str(num) + "\t" + ' '.join(new_query) + "\n")

queries_out.close()
queries_in.close()
