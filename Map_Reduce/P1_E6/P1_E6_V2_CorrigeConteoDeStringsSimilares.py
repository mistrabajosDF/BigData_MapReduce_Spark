import os
import difflib
from MRE import Job

'''
Una empresa proveedora de internet realizó una encuesta para conocer el grado de
satisfacción de sus clientes, en un formulario web los clientes debían completar un
campo con los textos "Muy satisfecho", "Algo satisfecho", "Poco satisfecho",
“Disconforme” o "Muy disconforme". Utilice el dataset Encuesta para saber cuántos
clientes están en cada una de las cinco categorías.
'''

root_path = "C:/Users/Usuario/Desktop/CABD/P1_E6/"

inputDir = root_path + "input/"
outputDir = root_path + "output2/"

categorias = [
    "muy satisfecho",
    "algo satisfecho",
    "poco satisfecho",
    "disconforme",
    "muy disconforme"
]

def normalizar_respuesta(resp):
    """ Devuelve la categoría más parecida a la respuesta ingresada """
    resp = resp.strip().lower()
    if not resp:
        return None
    # Buscar la mejor coincidencia entre las categorías
	#resp (sring analizado), categorias (las opciones)
	#n = cantidad de resultados posibles, 0.6 (60% igual)   
    match = difflib.get_close_matches(resp, categorias, n=1, cutoff=0.6)
    if match:
        return match[0]
    else:
        return resp  # si no se parece a nada, lo dejamos "tal cual"

def fmap(key, value, context):
    categoria = normalizar_respuesta(value)
    if categoria:
        context.write(categoria, 1)

def fred(key, values, context):
    total = 0
    for v in values:
        total += 1
    context.write(key, total)

# Crear carpeta de salida si no existe
if not os.path.exists(outputDir):
    os.makedirs(outputDir)

job = Job(inputDir, outputDir, fmap, fred)
success = job.waitForCompletion()
