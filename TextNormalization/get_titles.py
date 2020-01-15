doc = open("docs.tsv", "r")
titles = open("titles.tsv", "w")

for line in doc:
    parts = line.split("\t")
    titles.write(parts[0] + "\t" + str.lower(parts[1]) + "\n")

doc.close()
titles.close()
