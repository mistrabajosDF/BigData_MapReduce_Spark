import os
from MRE import Job
from datetime import date

'''
El dataset Inversionistas posee los nombres, dni, fecha de nacimiento (día, mes y año
como campos separados) e importe invertido por diferentes personas en la apertura
de un nuevo negocio en la ciudad. Se desea saber:
b. El total del importe invertido por todos los inversionistas
'''

# Ruta base del ejercicio
root_path = "C:/Users/Usuario/Desktop/CABD/P1_E7/"

inputDir = root_path + "input/"
outputDir = root_path + "output/"

# Crear carpeta de salida si no existe
if not os.path.exists(outputDir):
    os.makedirs(outputDir)

def fmap_importe(key, value, context):
    *_, importe = value.split()
    context.write("total_importe", int(importe))

def fred_importe(key, values, context):
    total = sum(values)
    context.write("total_importe", total)

job2 = Job(inputDir, outputDir + "importe/", fmap_importe, fred_importe)
job2.waitForCompletion()

