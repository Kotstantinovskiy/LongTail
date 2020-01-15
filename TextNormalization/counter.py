file = open("stem_docs.csv", "r")
out = open("tmp.txt", "w")

N = 0
lenAll = 0
for line in file:
    N = N + 1
    lenAll = lenAll + len(line.split("\t")[1].split(" ")) + len(line.split("\t")[2].split(" "))

out.write(str(N) + " " + str(lenAll))
file.close()
out.close()