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

docs_in = open("doc_in_1.tsv", "r")
docs_out = open("doc_out_1_1.tsv", "w")

for i, line in enumerate(docs_in):
	if i <= 94859:
		continue
			
	splits = line.split("\t")
    
	num = int(splits[0])
	title = str.lower(splits[1])
	text = str.lower(splits[2])
    	
	try:
		title_tokens = nlp(title)
		text_tokens = nlp(text)
	except:
		print("Error:", i)
		continue
    
	new_title = []
	for token in title_tokens:
		if (token.text in stopwords_english) or (token.text in stopwords_russian):
			continue
	
		lemmas = my_stem.lemmatize(token.text)
		word = ''.join(list(filter(lambda x: len(x) != 0, list(map(filter_symbols, lemmas)))))
		stemmer.stem(word)
		new_title.append(word)
    
	new_text = []
	for token in text_tokens:
		if (token.text in stopwords_english) or (token.text in stopwords_russian):
			continue
	
		lemmas = my_stem.lemmatize(token.text)
		word = ''.join(list(filter(lambda x: len(x) != 0, list(map(filter_symbols, lemmas)))))
		stemmer.stem(word)
		new_text.append(word)
    
	docs_out.write(str(num) + "\t" + ' '.join(new_title) + "\t" + ' '.join(new_text) + "\n")

docs_out.close()
docs_in.close()
