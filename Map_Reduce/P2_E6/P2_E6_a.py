from MRE import Job
'''
Cómo plantearía una solución MapReduce a los siguientes algoritmos secuenciales:
a.
i. entrada
textos: array [1..N] of string (dataset libros)
ii. algoritmo
a={}; b={}; N = len(textos)
for l in textos:
words = l.split()
for w in words:
a[w] = a[w]+1
for w in a.keys():
for l in lines:
words = l.split()
if w in words:
b[w]=b[w]+1
for k in a.keys():
print(k + " = " + str(a[w] * (N / b[w])))
'''
import re
import os

# ============
# Job A: Cuenta ocurrencias (TF) y documentos (DF) por palabra
# ============
def fmap_word_counts(key, content, context):
	"""
	Mapper para contar palabras
	key: puede ser offset o nombre de archivo
	content: línea del archivo
	"""
	try:
		# El content viene línea por línea, no archivo completo
		# Necesitamos identificar si es título o contenido
		line = content.strip()
		
		if not line:
			return
		
		# Si la línea está en mayúsculas, probablemente sea un título
		if line.isupper() and len(line.split()) <= 10:
			# Es un título, usarlo como doc_id pero no procesarlo
			context.write("__TITLE__", line)  # marcador especial para títulos
		else:
			# Es contenido, procesarlo
			# Usar el key como identificador del documento
			doc_id = str(key)  # convertir a string por si es offset
			
			# Normalizar texto
			text = line.lower()
			text = re.sub(r'[^\w\s]', ' ', text)
			text = re.sub(r'\s+', ' ', text).strip()
			
			# Procesar palabras
			words = text.split()
			for word in words:
				if word and len(word) > 1:
					# Emitir: palabra -> (1, doc_id)
					context.write(word, (1, doc_id))
					
	except Exception as e:
		print(f"Error en mapper: {e}")

def fred_word_counts(word, values, context):
	"""
	Reducer para contar TF y DF
	word: palabra clave
	values: iterador de tuplas (1, doc_id)
	"""
	try:
		# Ignorar títulos
		if word == "__TITLE__":
			return
		
		tf = 0
		docs = set()
		
		# MRE usa ValuesIterator que solo se puede iterar UNA vez
		for count, doc_id in values:
			tf += count
			docs.add(doc_id)
		
		df = len(docs)
		
		# Emitir resultado
		context.write(word, (tf, df))
		
	except Exception as e:
		print(f"Error en reducer word_counts para '{word}': {e}")

# ============
# Job B: Calcula TF-IDF
# ============
def fmap_tf_idf(word, tf_df_data, context):
	"""
	Mapper para TF-IDF: lee los datos del Job A y los pasa al reducer
	"""
	try:
		# Los datos vienen como string del archivo intermedio
		# Formato esperado: "tf\tdf" donde tf y df son números
		context.write(word, tf_df_data)
	except Exception as e:
		print(f"Error en mapper TF-IDF: {e}")

def fred_tf_idf(word, values, context):
	"""
	Reducer para calcular TF-IDF
	"""
	try:
		N = 100  # Número total de documentos - ajustar según dataset
		
		# Procesar valores del archivo intermedio
		for value in values:
			# El value debería ser un string como "1\t1" (tf, df separados por tab)
			if isinstance(value, str):
				parts = value.strip().split('\t')
				if len(parts) >= 2:
					try:
						tf = int(parts[0])
						df = int(parts[1])
						
						if df > 0:
							tfidf = tf * (N / df)
							context.write(word, tfidf)
						else:
							print(f"DF=0 para palabra '{word}'")
					except ValueError:
						print(f"Error parseando números para '{word}': {value}")
				else:
					print(f"Formato incorrecto para '{word}': {value}")
			else:
				print(f"Tipo inesperado para '{word}': {type(value)} = {value}")
			
			break  # Solo procesar el primer valor
			
	except Exception as e:
		print(f"Error en reducer TF-IDF para '{word}': {e}")

# ============
# Pipeline principal
# ============
if __name__ == "__main__":
	inputDir = "input/"
	outputDirA = "output_word_counts/"
	outputDirB = "output_tf_idf/"
	
	print("=== PROCESAMIENTO TF-IDF CON MRE ===")
	
	try:
		print("Ejecutando Job A (conteo TF/DF)...")
		jobA = Job(inputDir, outputDirA, fmap_word_counts, fred_word_counts)
		
		# Configurar parámetros opcionales si es necesario
		# jobA.setIntermDir("temp_a/")
		
		resultA = jobA.waitForCompletion()
		print(f"Job A resultado: {resultA}")
		
		if resultA:
			print("Job A completado")
			
			# Verificar archivos de salida
			if os.path.exists(outputDirA):
				files = os.listdir(outputDirA)
				print(f"Archivos en {outputDirA}: {files}")
				
				# Mostrar contenido del archivo de salida
				output_file = os.path.join(outputDirA, "output.txt")
				if os.path.exists(output_file):
					size = os.path.getsize(output_file)
					print(f"Tamaño de output.txt: {size} bytes")
					
					if size > 0:
						print("Primeras 10 líneas del Job A:")
						try:
							with open(output_file, 'r', encoding='utf-8') as f:
								for i, line in enumerate(f):
									if i >= 10:
										break
									print(f"  {line.strip()}")
						except Exception as e:
							print(f"Error leyendo archivo: {e}")
					else:
						print("El archivo output.txt está vacío")
				else:
					print("No se encontró output.txt")
			
			print("Ejecutando Job B (TF-IDF)...")
			jobB = Job(outputDirA, outputDirB, fmap_tf_idf, fred_tf_idf)
			
			resultB = jobB.waitForCompletion()
			print(f"Job B resultado: {resultB}")
			
			if resultB:
				print("Job B completado")
				
				# Verificar archivos finales
				if os.path.exists(outputDirB):
					files = os.listdir(outputDirB)
					print(f"Archivos en {outputDirB}: {files}")
					
					output_file = os.path.join(outputDirB, "output.txt")
					if os.path.exists(output_file):
						size = os.path.getsize(output_file)
						print(f"Tamaño final: {size} bytes")
						
						if size > 0:
							print("Primeras 10 líneas del resultado final:")
							try:
								with open(output_file, 'r', encoding='utf-8') as f:
									for i, line in enumerate(f):
										if i >= 10:
											break
										print(f"  {line.strip()}")
							except Exception as e:
								print(f"Error leyendo resultado: {e}")
						else:
							print("El archivo final está vacío")
			else:
				print("Error en Job B")
		else:
			print("Error en Job A")
			
	except Exception as e:
		print(f"Error en pipeline: {e}")
		
	print("=== FIN DEL PROCESAMIENTO ===")
