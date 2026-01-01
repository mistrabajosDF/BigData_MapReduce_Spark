import os

# Buscar top 20 palabras mas usadas 

from MRE import Job

root_path = "C:/Users/Usuario/Desktop/CABD/P1_E3/"
inputDir = root_path + "output/"
outputDir = root_path + "top20_output/"

def fmap_top20(key, value, context):
	""" Lee el wordcount y emite con formato especial
	Formato de entrada esperado: palabra\tfrequencia
	"""
	if not value or not value.strip():
		return
		
	parts = value.strip().split('\t')
	if len(parts) == 2:
		word = parts[0].strip().strip('"')
		count = int(parts[1])
		if count > 0:
			formatted_value = f"{count:010d}|{word}"
			context.write("all_words", formatted_value)

def fred_top20(key, values, context):
	word_counts = []
	for value in values:
		if isinstance(value, str) and '|' in value:
			parts = value.split('|', 1)
			if len(parts) == 2:
				count = int(parts[0])
				word = parts[1]
				word_counts.append((count, word))
	if not word_counts:
		return
	word_counts.sort(key=lambda x: x[0], reverse=True)
	top_20 = word_counts[:20]
	for count, word in top_20:
		context.write(word, count)

if not os.path.exists(outputDir):
	os.makedirs(outputDir)

job = Job(inputDir, outputDir, fmap_top20, fred_top20)
job.waitForCompletion()
