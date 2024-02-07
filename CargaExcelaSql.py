import pandas as pd
from pathlib import Path 
from sqlalchemy import create_engine, text

# funcion para leer la ruta
def leer_ruta_del_archivo():
    ruta_ingresada = input("Paso 1: Ingrese la ruta del archivo: ")
    ruta = Path(ruta_ingresada) 
    return ruta

# funcion para validar si existe el archivo
def validar_existe_archivo(ruta):
    ruta_name = ruta.name
    if ruta.exists() == True:
        print(f"Paso 2: se valida que existe el archivo {ruta_name}")
    elif ruta.exists() == False:
        print(f"Paso 2: No existe el archivo {ruta_name}")
    else:
        print(f"ruta error")

# funcion para leer el archivo en un dataframe
def leer_archivo_en_un_df(ruta):
    df = pd.read_excel(ruta)
    return df

# funcion para leer el nombre de la tabla en sql
def leer_nombre_tabla():
    nombre_tabla = input("Paso 3: ingrese el nombre de la tabla sql: ")
    nombre_tabla = nombre_tabla.replace(" ","_").replace(".","_").replace("*","_").replace(",","_")
    nombre_tabla = 'FINANZAS_' + nombre_tabla #Si deseas actualiza
    print(f"se cargará la información en la tabla: {nombre_tabla}")
    return nombre_tabla

def cargar_informacion(df,nombre_tabla):
    # Poner el nombre del servidor
    nombre_servidor = 'SVRBI' #Actualizar con el nombre de tu Servidor
    # Poner el nombre de la base de datos
    nombre_base_datos = 'BDANALYTICS' #Actualizar con el nombre de la base de datos a la que quieres cargar la información
    linea_coneccion = f"mssql+pyodbc://{nombre_servidor}/{nombre_base_datos}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server"
    coneccion_bd = create_engine(linea_coneccion) 
    coneccion = coneccion_bd.connect() 
    df.to_sql(nombre_tabla,con=coneccion, index=False,if_exists="fail") 
    coneccion.commit()
    coneccion.close() 
    print(f"Se cargo la tabla {nombre_tabla} correctamente")


# funcion iniciar
def inicio_carga_excel():
    ruta = leer_ruta_del_archivo()
    validar_existe_archivo(ruta)
    df = leer_archivo_en_un_df(ruta)
    nombre_tabla = leer_nombre_tabla()
    cargar_informacion(df,nombre_tabla)
    print("fin")

inicio_carga_excel()
 