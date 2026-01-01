import os
from MRE import Job
from datetime import date

'''
El dataset Inversionistas posee los nombres, dni, fecha de nacimiento (día, mes y año
como campos separados) e importe invertido por diferentes personas en la apertura
de un nuevo negocio en la ciudad. Se desea saber:
a. El nombre del inversionista más joven
'''

root_path = "C:/Users/Usuario/Desktop/CABD/P1_E7/"
inputDir = root_path + "input/"
outputDir = root_path + "output/"

if not os.path.exists(outputDir):
    os.makedirs(outputDir)

def fmap_joven(key, value, context):
    partes = value.split("\t")
    if len(partes) != 5:
        print(f"[SALTEANDO FILA] Archivo: {key}, Contenido: {value}")
        return
    nombre, d, m, a, importe = partes
    fecha = (int(a), int(m), int(d))  # año, mes, día
    #print(f"[PROCESANDO==============================] {nombre}, fecha: {fecha}")
    context.write("mas_joven", (fecha, nombre))

def fred_joven(key, values, context):
    joven = max(values)  # mayor fecha → más joven
    context.write("nombre_mas_joven", joven[1])

job1 = Job(inputDir, outputDir + "joven/", fmap_joven, fred_joven)
job1.waitForCompletion()


