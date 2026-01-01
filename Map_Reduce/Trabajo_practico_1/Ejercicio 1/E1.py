''' Análisis de uso de un videojuego donde jugadores se conectan a diario para combatir con otros. En un combate, un jugador “retador”
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
1) El jugador más “retador” y el jugador más “retado”.
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

#======PRIMER JOB======
def fmap(key, value, context):
	"""Separa el archivo recibido y envia:
	jugador + R + 1, por cada retador;
	jugador + D + 1, por cada desafiado"""
	parts = value.strip().split("\t") 
	context.write(key, ("R", 1))     
	context.write(parts[0], ("D", 1))

def fcomb(key, values, context):
	"""Combina la salida de cada map, contabilizando las veces que fue 
	retador y desafiado para cada jugador en ese nodo"""
	count_R = 0
	count_D = 0
	for tag, n in values:
		if tag == "R":
			count_R += n
		elif tag == "D":
			count_D += n
	if count_R > 0:
		context.write(key, ("R", count_R))
	if count_D > 0:
		context.write(key, ("D", count_D))

def fred(key, values, context):
	"""Suma todas las veces que un jugador que fue retador y desafiado
	y las guarda con el fomato:
	nº jugador /t nº de veces que fue retador /t nº de veces que fue desafiado"""
	count_R = 0
	count_D = 0
	for tag, n in values:
		if tag == "R":
			count_R += n
		elif tag == "D":
			count_D += n
	context.write(key, (count_R, count_D))

job1 = Job(inputDir, outputDir, fmap, fred)
job1.setCombiner(fcomb)
job1.waitForCompletion()

#======SEGUNDO JOB======
def fmap_max(key, value, context):
	"""Parte de la salida de job1, y envía, para cada jugador, su 
	conteo de veces que fue retador y desafiado"""
	parts = value.strip().split("\t")
	count_R, count_D = map(int, parts)
	context.write("max_retador", (key, count_R))
	context.write("max_desafiado",  (key, count_D))

def fred_max(key, values, context):
	"""Busca el jugador mas veces retado y desafiado y lo guarda con
	el formato: max_retador/max_desafiado /t nº jugador /t nº de veces"""
	max_val = -1
	jugador_max = None
	for jugador, n in values:
		if n > max_val:
			max_val = n
			jugador_max = jugador
	context.write(key, str(jugador_max) + "\t" + str(max_val))

job2 = Job(outputDir, maxOutputDir, fmap_max, fred_max)
job2.waitForCompletion()
