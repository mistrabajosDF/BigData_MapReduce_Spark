'''Análisis de uso de un videojuego donde jugadores se conectan a diario para combatir con otros. En un combate, un jugador “retador”
elije un oponente (jugador “retado”) y como resultado del combate el retador obtiene un puntaje y el tiempo en segundos que duró el combate. Por cada combate, los servidores del videojuego almacena la siguiente información:

• ID_Jugador_Retador

• ID_Jugador_Retado

• Puntaje obtenido (por el retador)

• Tiempo del combate en segundos

Todos los jugadores pueden participar cuántas veces deseen y por cada combate obtienen un puntaje y también se registra el tiempo que duró el combate. Un jugador puede combatir todas las veces que quiera.

Un fragmento del dataset de este jugador podría ser:
ID_Jug_Retador ID_Jugador_Retado Puntos Tiempo
231            492                1054   621
231            492                2068   504
231            98                 789    302
492             01                5462   955

Se quieren obtener algunas estadísticas para armar el ranking semanal. Al finalizar la semana se desea saber.
2) El jugador que más puntos obtuvo en promedio :
PPᵢ = (puntaje_total_de_todos_los_combatesᵢ + 1) / (cantidad_de_combates_como_retadorᵢ + 1)
(el +1 en el numerador y en el denominador es para evitar divisiones por cero en el caso que un jugador no haya “retado” a nadie, en cuyo caso tendrá el puntaje mínimo de 1).
'''

import os
from MRE import Job

root_path = "./"
inputDir = root_path + "input/"
outputDir = root_path + "output/"
maxOutputDir = root_path + "max_output/"

if not os.path.exists(outputDir):
	os.makedirs(outputDir)
if not os.path.exists(maxOutputDir):
	os.makedirs(maxOutputDir)

#======JOB 1======
def fmap_avg(key, value, context):
	"""Envía el jugador, puntaje y cant de combates (1)"""
	parts = value.strip().split("\t")
	context.write(key, (int(parts[1]), 1))

def fcomb_avg(key, values, context):
	"""Contabiliza y envía el jugador, puntaje y cant de combates
	de un nodo map"""
	total_puntaje = 0
	total_combates = 0
	for p, n in values:
		total_puntaje += p
		total_combates += n
	context.write(key, (total_puntaje, total_combates))

def fred_avg(key, values, context):
	"""Calcula y envia el promedio de un jugador"""
	total_puntaje = 0
	total_combates = 0
	for p, n in values:
		total_puntaje += p
		total_combates += n
	puntaje_promedio = (total_puntaje + 1) / (total_combates + 1) 
	context.write(key, puntaje_promedio)

job1 = Job(inputDir, outputDir, fmap_avg, fred_avg)
job1.setCombiner(fcomb_avg)
job1.waitForCompletion()

#======JOB 2======
def fmap_max_avg(key, value, context):
	"""	Recibe pares (jugador, promedio), y lo envía al combiner"""
	parts = value.strip().split("\t")
	context.write("P", (key, float(parts[0])))

def fcomb_job2(key, values, context):
	"""	Recibe pares (P, jugador, promedio), busca el máximo promedio 
	local y lo envía"""
	jugador_max = None
	max_promedio = -1
	for jugador, puntaje_promedio in values:
		if puntaje_promedio > max_promedio:
			max_promedio = puntaje_promedio
			jugador_max = jugador
	context.write("P", (jugador_max, max_promedio))
	
def fred_max_avg(key, values, context):
	"""Calcula el jugador con mayor promedio y lo escribe con formato:
	nº jugador, promedio"""
	max_pp = -1
	jugador_max = None
	for jugador, pp in values:
		if pp > max_pp:
			max_pp = pp
			jugador_max = jugador
	context.write(str(jugador_max), "\t" + str(max_pp))

job2 = Job(outputDir, maxOutputDir, fmap_max_avg, fred_max_avg)
job1.setCombiner(fcomb_job2)
job2.waitForCompletion()
