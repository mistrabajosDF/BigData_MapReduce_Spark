import os
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
outputDir = root_path + "output/"

def fmap(key, value, context):
    """
    value es una línea del archivo (una respuesta del cliente).
    Normalizamos a minúsculas para evitar problemas de mayúsculas.
    """
    respuesta = value.strip().lower()
    if respuesta:  # descartar líneas vacías
        context.write(respuesta, 1)

def fred(key, values, context):
    """ Suma la cantidad de clientes por categoría """
    total = 0
    for v in values:
        total += 1
    context.write(key, total)

# Crear carpeta de salida si no existe
if not os.path.exists(outputDir):
    os.makedirs(outputDir)

job = Job(inputDir, outputDir, fmap, fred)
success = job.waitForCompletion()
