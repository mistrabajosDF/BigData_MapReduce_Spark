'''
4) Modifique el proyecto WordCount para contar cuántas vocales, consonantes, dígitos,
espacios y otros caracteres posee el data set Libros.
'''
import os
from MRE import Job

root_path = "C:/Users/Usuario/Desktop/CABD/P1_E4/"

inputDir = root_path + "input/"
outputDir = root_path + "output/"

def fmap(key, value, context):
    """
    Clasifica cada caracter del texto en:
    - vocal
    - consonante
    - digito
    - espacio
    - otro
    """
    for ch in value.lower():  # pasamos a minúsculas para simplificar
        if ch in "aeiouáéíóú":
            context.write("vocales", 1)
        elif ch.isalpha():
            context.write("consonantes", 1)
        elif ch.isdigit():
            context.write("dígitos", 1)
        elif ch.isspace():
            context.write("espacios", 1)
        else:
            context.write("otros", 1)

def fred(key, values, context):
    """ Suma la cantidad total de cada categoría """
    total = 0
    for v in values:
        total += 1
    context.write(key, total)

# Crear carpeta de salida si no existe
if not os.path.exists(outputDir):
    os.makedirs(outputDir)

job = Job(inputDir, outputDir, fmap, fred)
success = job.waitForCompletion()
