from MRE import Job

import os, random

'''
Si contáramos con un cluster donde podemos configurar 100 nodos para la tarea de
reduce ¿De qué manera se podrían usar esos 100 nodos en el ejemplo de los eventos
POSITIVO, NEGATIVO y NEUTRO visto en la teoría?
'''

root_path = "C:/Users/Usuario/Desktop/CABD/P1_E8/"
inputDir = root_path + "input/"
tmpDir = root_path + "tmp_job1/"
outputDir = root_path + "output_final/"

# -------- Job 1 --------
def fmap1(key, value, context):
    resp = value.strip().lower()
    if resp in ["positivo", "negativo", "neutro"]:
        bucket = random.randint(1, 100)
        context.write(f"{resp}|{bucket}", 1)

def fred1(key, values, context):
    total = sum(int(v) for v in values)
    context.write(key, total)

if not os.path.exists(tmpDir):
    os.makedirs(tmpDir)

job1 = Job(inputDir, tmpDir, fmap1, fred1)
job1.waitForCompletion()

# -------- Job 2 --------
def fmap2(key, value, context):
    tipo, bucket = key.split("|")
    context.write(tipo, value)

def fred2(key, values, context):
    total = sum(int(v) for v in values)  # convertir cada value a entero
    context.write(key, total)


if not os.path.exists(outputDir):
    os.makedirs(outputDir)

job2 = Job(tmpDir, outputDir, fmap2, fred2)
job2.waitForCompletion()
