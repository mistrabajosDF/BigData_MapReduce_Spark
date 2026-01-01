import os
from MRE import Job
from datetime import date

'''
El dataset Inversionistas posee los nombres, dni, fecha de nacimiento (día, mes y año
como campos separados) e importe invertido por diferentes personas en la apertura
de un nuevo negocio en la ciudad. Se desea saber:
c. El promedio de edad
'''

root_path = "C:/Users/Usuario/Desktop/CABD/P1_E7/"
inputDir = root_path + "input/"
outputDir = root_path + "output/"

if not os.path.exists(outputDir):
    os.makedirs(outputDir)

anio_actual = date.today().year  # año actual

def fmap_edad(key, value, context):
   
    partes = value.split("\t")
    
    if len(partes) != 5:
        print(f"[SALTEANDO FILA] Archivo: {key}, Esperaba 6 campos, encontré {len(partes)}")
        print(f"[SALTEANDO FILA] Contenido: {repr(value)}")
        return
    
    try:
        nombre, d, m, a, importe = partes
        #print(f"[DEBUG] Campos extraídos - Nombre: {nombre}, Día: {d}, Mes: {m}, Año: {a}, Importe: {importe}")
        
        edad = anio_actual - int(a)
        #print(f"[PROCESANDO] {nombre}, edad: {edad}")
        context.write("edades", (edad, 1))
    except ValueError as e:
        print(f"[ERROR DE CONVERSIÓN] Archivo: {key}, Contenido: {value}, Error: {e}")
    except Exception as e:
        print(f"[ERROR GENERAL] Archivo: {key}, Contenido: {value}, Error: {e}")

def fred_edad(key, values, context):
    suma, count = 0, 0
    for edad, c in values:
        suma += edad
        count += c
    
    promedio = suma / count if count > 0 else 0
    context.write("promedio_edad", promedio)

# Ejecutar el job
job3 = Job(inputDir, outputDir + "edad/", fmap_edad, fred_edad)
job3.waitForCompletion()
