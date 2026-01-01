'''
El dataset Libros provisto por la cátedra almacena libros cada uno en un archivo separado. 
Dentro de cada archivo, la primera línea tiene el título del libro y luego en las líneas siguientes un párrafo por línea. 
Ejecute el proyecto WordCount dado por la cátedra para saber cuántas veces es utilizada cada palabra.
'''

import os
from MRE import Job

root_path = "C:/Users/Usuario/Desktop/CABD/P2_E2/"

inputDir = root_path + "input/"
outputDir = root_path + "output/"

def fmap(key, value, context):
    words = value.split()
    for w in words:
        context.write(w, 1)

def fcomb(key, values, context):
    c = 0
    for v in values:
        c += int(v)
    context.write(key, c)

def fred(key, values, context):
    total = 0
    for v in values:
        total += int(v)   
    context.write(key, total)

job = Job(inputDir, outputDir, fmap, fred)
job.setCombiner(fcomb)     # <-- así se registra el combiner en este MRE
success = job.waitForCompletion()

'''
Del map sale: gato: 1, gato: 1, gato: 1, perro: 1
Del combiner sale: gato: 3, perro: 1
Al reduce le llegan todos los combiner de cada map y los suma: gato: [3, 5, 2], perro: [1, 4]
'''

