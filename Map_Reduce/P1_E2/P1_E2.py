'''
El dataset Libros provisto por la cátedra almacena libros cada uno en un archivo separado. 
Dentro de cada archivo, la primera línea tiene el título del libro y luego en las líneas siguientes un párrafo por línea. 
Ejecute el proyecto WordCount dado por la cátedra para saber cuántas veces es utilizada cada palabra.
'''

import os
from MRE import Job

# Ruta base donde está este script
root_path = "C:/Users/Usuario/Desktop/CABD/P1_E2/"

inputDir = root_path + "input/"
outputDir = root_path + "output/"

def fmap(key, value, context):
    words = value.split()
    for w in words:
        context.write(w, 1)

def fred(key, values, context):
    c = 0
    for v in values:
        c = c + 1
    context.write(key, c)

# Crear carpeta de salida si no existe
if not os.path.exists(outputDir):
    os.makedirs(outputDir)

job = Job(inputDir, outputDir, fmap, fred)
success = job.waitForCompletion()
