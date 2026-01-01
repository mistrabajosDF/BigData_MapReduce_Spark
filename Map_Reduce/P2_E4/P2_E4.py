import os

'''
Utilice el dataset Libros para implementar una aplicación MapReduce que devuelva
como salida todos los párrafos que tienen una longitud mayor al promedio.
'''
from MRE import Job

root_path = "C:/Users/Usuario/Desktop/CABD/P2_E4/"
inputDir = root_path + "input/"
outputDir1 = root_path + "output1/"
outputDir2 = root_path + "output2/"

# --- Función para leer archivos con diferentes codificaciones ---
def leer_archivo_con_encoding(filepath):
	"""Intenta leer un archivo probando diferentes codificaciones"""
	encodings = ['utf-8', 'latin1', 'cp1252', 'iso-8859-1']
	
	for encoding in encodings:
		try:
			with open(filepath, 'r', encoding=encoding) as f:
				content = f.read()
				print(f"Archivo {os.path.basename(filepath)} leído con codificación: {encoding}")
				return content
		except UnicodeDecodeError:
			continue
	
	with open(filepath, 'r', encoding='latin1', errors='replace') as f:
		return f.read()

# --- Job 1: calcular promedio global ---
def fmap_len(key, value, context):
	'''Divide la entrada en parrafos, y por cada parrafo, envia (stats, cantidad de palabras, 1)'''
	paragraphs = [p.strip() for p in value.split('\n') if p.strip()]
	
	for paragraph in paragraphs:
		if paragraph:  
			words = paragraph.split()
			if words: 
				context.write("stats", (len(words), 1)) #Tantas palabras en un parrafo

def fred_len(key, values, context):
	'''Recibe cant de palabras/parrafo y hace el promedio'''
	total_words = 0
	total_parrafos = 0
	
	for v in values:
		w, c = v
		total_words += w
		total_parrafos += c
	
	if total_parrafos > 0:
		promedio = total_words / total_parrafos
		context.write("promedio", promedio)
	else:
		context.write("promedio", 0.0)

# --- Job 2: filtrar párrafos mayores al promedio ---
def fmap_parrafos(key, value, context):
	'''Divide el contenido en parrafos y cuenta las palabras'''
	paragraphs = [p.strip() for p in value.split('\n') if p.strip()]
	
	for paragraph in paragraphs:
		if paragraph:
			words = paragraph.split()
			if words:
				context.write(paragraph, len(words))


def fred_parrafos(key, values, context):
	'''Busca parrafos mayores al promedio'''
	for longitud in values:
			if longitud > promedio_global:
				context.write("parrafo_largo", f"{key} (longitud: {longitud})")
				break 
		

def leer_promedio_desde_salida(output_dir):
    try:
        filepath = os.path.join(output_dir, "output.txt")
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                if line.startswith("promedio"):
                    promedio = float(line.split()[1])
                    return promedio
    except Exception as e:
        print(f"Error leyendo promedio: {e}")
    
    return 0.0

# --- Ejecutar los jobs ---
try:
	os.makedirs(outputDir1, exist_ok=True)
	os.makedirs(outputDir2, exist_ok=True)

	# Job 1: Calcular promedio
	job1 = Job(inputDir, outputDir1, fmap_len, fred_len)
	job1.waitForCompletion()

	# Leer el promedio desde la salida del job1
	promedio_global = leer_promedio_desde_salida(outputDir1)
	
	if promedio_global <= 0:
		print("ERROR: No se pudo calcular un promedio válido")

	else:	
		# Job 2: Párrafos mayores al promedio
		job2 = Job(inputDir, outputDir2, fmap_parrafos, fred_parrafos)
		job2.waitForCompletion()
		

except Exception as e:
	print(f"Error durante la ejecución: {e}")
	import traceback
	traceback.print_exc()
