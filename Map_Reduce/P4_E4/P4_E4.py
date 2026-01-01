from MRE import Job  

'''
Muchos cálculos aritméticos necesitan ordenar una serie de números para obtener su
resultado, como por ejemplo la mediana.
La mediana es el "número en el medio" de una lista ordenada de números.
3, 5, 7, 12, 13, 14, 21, 23, 23, 23, 23, 29, 39, 40, 56
Implemente una solución MapReduce que permita calcular la mediana de una serie de
valores. Use como prueba el dataset website para calcular la mediana del tiempo de
permanencia.
'''

import os


# ========== JOB 1: contar ocurrencias por duration ==========
def map_counts(k, v, context):
	"""Manda la duracion y 1 (Esa duracion aparecio una vez)"""
	parts = v.strip().split("\t")
	if len(parts) >= 2:
		try:
			duration = int(parts[1])
			context.write(duration, 1)
		except:
			print("DEBUG=======: Error en el formato de entrada.")
			return
	else:
		return


def combiner_counts(key, values, context):
	"""Si se repiten las duraciones, suma"""
	s = 0
	for x in values:
		s += int(x)
	context.write(key, s)


def reduce_counts(key, values, context):
	"""Cuenta la cantidad de veces que hay de cada una"""
	total = 0
	for v in values:
		total += int(v)
	context.write(key, total)

def shuffle_cmp_num(a, b):
	"""Compara y envia ordenado al reduce"""
	if a == b:
		return 0
	elif a < b:
		return -1
	else:
		return 1


def sort_cmp_num(a, b):
	return shuffle_cmp_num(a, b)


#JOB 2: calcular la mediana leyendo counts

def map_median(key, value, context):
	"""Solo emite"""
	#print("DEBUG======= key: ", key, " ", type (key))
	#print("DEBUG======= value: ", value, " ", type (value))
	context.write(int(key), int(value))


# Reducer que busca la mediana. Usamos un contenedor mutable para acumular estado entre llamadas
_cumulative = [0]   # lista para ser mutable en scope del reducer
_median_written = [False]


def reduce_median(key, values, context):
	target = context[0]
	cnt = 0
	for v in values:
		try:
			cnt += int(v)
		except:
			pass
	if cnt == 0:
		return
	_cumulative[0] += cnt
	if (not _median_written[0]) and (_cumulative[0] >= target):
		context.write("median", key)
		_median_written[0] = True

input_dir="input"
out_counts="out_counts"
out_median="out_median"

# Job 1
job1 = Job(input_dir, out_counts, map_counts, reduce_counts)
job1.setCombiner(combiner_counts)
job1.setShuffleCmp(shuffle_cmp_num)
job1.setSortCmp(sort_cmp_num)
print("Ejecutando job de counts...")
job1.waitForCompletion()
print("Job counts finalizado. Salida en:", out_counts)

# --- leer la salida para calcular N (total) ---
out_file = out_counts + "/output.txt"
if not os.path.isfile(out_file):
    raise Exception("No se encontró salida de counts: " + out_file)

totalN = 0
with open(out_file, "r", encoding="latin-1") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        parts = line.split("\t")
        try:
            cnt = int(parts[-1])
            totalN += cnt
        except:
            pass

if totalN == 0:
    print("No hay datos para calcular mediana.")
else:
    target = (totalN + 1) // 2
    print(f"Total N = {totalN}. Target index para la mediana = {target}")

    # --- Job 2: barrer en orden los duration con sus counts y localizar la mediana ---
    _cumulative[0] = 0
    _median_written[0] = False

    job2 = Job(out_counts, out_median, map_median, reduce_median)
    job2.setShuffleCmp(shuffle_cmp_num)
    job2.setSortCmp(sort_cmp_num)
    job2.setParams([target])
    print("Ejecutando job para hallar la mediana...")
    job2.waitForCompletion()
    print("Job mediana finalizado. Salida en:", out_median)

	
