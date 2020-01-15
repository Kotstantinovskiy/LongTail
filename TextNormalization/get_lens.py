doc_in = open("docs_norm.tsv", "r")
doc_out = open("lens.tsv", "w")

len_title = 0
len_text = 0
len_all = 0
for line in doc_in:
	parts = line.split("\t")
	len_title = len_title + len(parts[1].split(" "))
	len_text = len_text + len(parts[2].split(" "))
	len_all = len_all + len(parts[1].split(" ")) + len(parts[2].split(" "))

doc_out.write("TITLE: " + str(len_title) + "\n")
doc_out.write("TEXT: " + str(len_text) + "\n")
doc_out.write("ALL: " + str(len_all) + "\n")

doc_in.close()
doc_out.close()
