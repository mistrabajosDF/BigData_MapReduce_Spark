import os

# Buscar top 20 palabras mas usadas 

# Ruta del archivo de salida del WordCount
root_path = "C:/Users/Usuario/Desktop/CABD/P1_E2/"
output_dir = root_path + "output/"

def get_top20_words():
    """
    Lee el archivo de salida del WordCount y retorna el top 20 de palabras
    """
    word_counts = []
    
    # Buscar archivos de salida (generalmente empiezan con 'part-')
    output_files = [f for f in os.listdir(output_dir) if f.startswith('part-')]
    
    for filename in output_files:
        filepath = os.path.join(output_dir, filename)
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) == 2:
                    word = parts[0].strip('"')  # Remover comillas
                    try:
                        count = int(parts[1])
                        word_counts.append((count, word))
                    except ValueError:
                        continue  # Ignorar líneas mal formateadas
    
    # Ordenar por frecuencia (descendente) y tomar los primeros 20
    word_counts.sort(reverse=True)
    top_20 = word_counts[:20]
    
    return top_20

def save_top20_results(top_20):
    """
    Guarda los resultados del top 20 en un archivo
    """
    output_file = root_path + "top20_results.txt"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("=== TOP 20 PALABRAS MÁS USADAS ===\n\n")
        for i, (count, word) in enumerate(top_20, 1):
            f.write(f"{i:2d}. {word:<20} ({count:>6} veces)\n")
    
    print(f"Resultados guardados en: {output_file}")
    return output_file

# Ejecutar el análisis
if __name__ == "__main__":
    try:
        top_20 = get_top20_words()
        
        if top_20:
            print("=== TOP 20 PALABRAS MÁS USADAS ===\n")
            for i, (count, word) in enumerate(top_20, 1):
                print(f"{i:2d}. {word:<20} ({count:>6} veces)")
            
            # Guardar resultados
            save_top20_results(top_20)
            
        else:
            print("No se encontraron datos para procesar.")
            
    except Exception as e:
        print(f"Error: {e}")
