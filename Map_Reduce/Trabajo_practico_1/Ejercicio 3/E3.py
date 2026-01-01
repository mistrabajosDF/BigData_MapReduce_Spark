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

3) Todos los jugadores que “retaron” a más de H oponentes distintos (H es un parámetro de la consulta).
'''

import os
from MRE import Job

root_path = "./"
inputDir = root_path + "input/"
outputDir = root_path + "output_distintos/"

while True:
	try:
		H = int(input("Ingrese cantidad de oponentes distintos mínima a buscar (Entrega mayor a este valor, no igual): "))
		break
	except ValueError:
		print("ERROR: Tiene que ser un número entero...")

if not os.path.exists(outputDir):
	os.makedirs(outputDir)

def fmap_distintos(key, value, context):
	"""Manda el retador y el desafiado"""
	parts = value.strip().split("\t")
	context.write(key, parts[0])

def fred_distintos(key, values, context):
	"""Cuenta cantidad de desafiados distintos de cada jugador y, si es 
	mayor a H, lo envía con el formato: jugador, cant de oponentes"""
	vistos = []
	contador = 0
	for v in values:
		if v not in vistos:
			vistos.append(v)
			contador += 1
	if contador > context[0]:
		context.write(key, contador)

params = [H]
job = Job(inputDir, outputDir, fmap_distintos, fred_distintos)
job.setParams(params)
job.waitForCompletion()

