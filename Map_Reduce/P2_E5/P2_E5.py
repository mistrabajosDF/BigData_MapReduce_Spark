'''
El dataset website tiene información sobre el tiempo de permanencia de sus usuarios
en cada una de las páginas del sitio. El formato de los datos del dataset es:
<id_user, id_page, time>
Implemente una aplicación MapReduce, utilizando combiners en los casos que
considere necesario, que calcule
a. La página más visitada (la página en la que más tiempo permaneció) para cada
usuario
b. El usuario que más páginas distintas visitó
c. La página más visitada (en cuanto a cantidad de visitas, sin importar el tiempo
de permanencia) por todos los usuarios.
Indique como queda el DAG del proceso completo (las tres consultas)
'''
import os
from MRE import Job

root_path = "C:/Users/Usuario/Desktop/CABD/P2_E5/"
inputDir = root_path + "input/"
outputDirA = root_path + "outputA/"  # Página con más tiempo por usuario
outputDirB1 = root_path + "outputB1/"  # Cantidad de páginas distintas por usuario
outputDirB2 = root_path + "outputB2/"  # Usuario con más páginas
outputDirC1 = root_path + "outputC1/"  # Cantidad de visitas por página
outputDirC2 = root_path + "outputC2/"  # Página más visitada global

# -------------------- JOB A --------------------
# Página con mayor tiempo por usuario
def fmap_user_max_time(key, value, context):
	# key ya es el user
	user = key
	page, time = value.strip().split('\t')
	context.write(user, (page, int(time)))

def fred_user_max_time(user, values, context):
	'''Recibe la lista de páginas y tiempos por usuario, acumula los tiempos y devuelve la página con máximo tiempo'''
	tiempos_por_pagina = {}
	for page, time in values:
		if page not in tiempos_por_pagina:
			tiempos_por_pagina[page] = 0
		tiempos_por_pagina[page] += int(time)

	max_page = max(tiempos_por_pagina, key=lambda p: tiempos_por_pagina[p])
	max_time = tiempos_por_pagina[max_page]

	context.write(user, max_page)

# -------------------- JOB B --------------------
# Usuario que más páginas distintas visitó
def fmap_user_pages(key, value, context):
	# key ya es el user
	user = key
	page, time = value.strip().split('\t')
	context.write(user, page)

def comb_user_pages(user, pages, context):
	for page in set(pages):
		context.write(user, page)

def fred_user_pages_count(user, pages, context):
	unique_pages = set()
	for page in pages:
		unique_pages.add(page)
	context.write(user, len(unique_pages))

def fmap_max_user(user, count, context):
	context.write("max", (user, count))

def fred_max_user(key, values, context):
	max_count = 0
	for user, count in values:
		if int(count) > int(max_count):
			max_count = int(count)
			max_user = user
	context.write(max_user, max_count)

# -------------------- JOB C --------------------
# Página más visitada en cantidad de veces
def fmap_page_visits(user, value, context):
	page, time = value.strip().split('\t')
	context.write(page, 1)

def comb_page_visits(page, counts, context):
	context.write(page, sum(counts))

def fred_page_visits(page, counts, context):
	total = sum(counts)
	context.write(page, total)

def fmap_max_page(page, total, context):
	context.write("max", (page, total))

def fred_max_page(key, values, context):
	max_page = None
	max_count = 0
	for page, total in values:
		if int(total) > int(max_count):
			max_count = int(total)
			max_page = page
	context.write(max_page, max_count)

# -------------------- EJECUCIÓN --------------------
try:
	# Crear carpetas de salida
	for d in [outputDirA, outputDirB1, outputDirB2, outputDirC1, outputDirC2]:
		os.makedirs(d, exist_ok=True)

	jobA = Job(inputDir, outputDirA, fmap_user_max_time, fred_user_max_time)
	jobA.waitForCompletion()

	jobB1 = Job(inputDir, outputDirB1, fmap_user_pages, fred_user_pages_count)
	jobB1.setCombiner(comb_user_pages)
	jobB1.waitForCompletion()

	jobB2 = Job(outputDirB1, outputDirB2, fmap_max_user, fred_max_user)
	jobB2.waitForCompletion()

	jobC1 = Job(inputDir, outputDirC1, fmap_page_visits, fred_page_visits)
	jobC1.setCombiner(comb_page_visits)
	jobC1.waitForCompletion()

	jobC2 = Job(outputDirC1, outputDirC2, fmap_max_page, fred_max_page)
	jobC2.waitForCompletion()

	print("Salidas:")
	print(f"- Job A: {outputDirA}")
	print(f"- Job B: {outputDirB2}")
	print(f"- Job C: {outputDirC2}")

except Exception as e:
	print(f"Error durante la ejecución: {e}")
	import traceback
	traceback.print_exc()
