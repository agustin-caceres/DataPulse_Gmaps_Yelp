import pandas as pd

# Opciones predefinidas
categorias = [
    'JAPONESA - ASIATICA', 'BARES - CERVECERIAS - TAPAS', 'RESTAURANTES GENERALES',
    'MEDITERRANEA', 'PIZZERIAS', 'GRILL - ASADOS - CARNES', 'MEXICANA',
    'COMIDA RAPIDA', 'CAFETERIAS - COMIDAS LIGERAS', 'COCINA INTERNACIONAL',
    'DIETA - VEGANA - ENSALADAS'
]

caracteristicas = [
    'ACEPTA TARJETA DE CREDITO', 'SERVICIO DE DELIVERY', 'SERVICIO PARA LLEVAR',
    'ACCESIBILIDAD SILLAS DE RUEDA', 'ESTACIONAMIENTO BICICLETAS',
    'APROPIADO PARA NIÑOS', 'ACEPTA MASCOTAS'
]

kms = [25, 37, 50]

estados = ['AZ', 'CA', 'DE', 'FL', 'ID', 'IL', 'IN', 'LA', 'MO', 'NV', 'PA', 'TN']

ciudades_por_estado = {
    'AZ': ['Corona De Tucson', 'Marana', 'Sahuarita', 'Vail', 'Valencia West', 'Catalina', 'Drexel Heights', 'Casa Adobes', 'Oro Valley', 'Tucson'],
    'CA': ['Meridian', 'Sparks', 'Santa Clara', 'Carpinteria', 'Summerland', 'Truckee', 'Montecito', 'Santa Barbara', 'Goleta', 'Isla Vista'],
    'DE': ['Hockessin', 'Greenville', 'Newport', 'Montchanin', 'Wilmington Manor', 'Elsmere', 'Talleyville', 'Christiana', 'Newark', 'Bellefonte'],
    'FL': ['Treasure Island', 'Palmetto', 'North Redington Beach', 'Mango', 'Southwest Tampa', 'Saint Petersburg', 'Bayonet Point', 'Clearwater', 'Lithia', 'Nashville'],
    'ID': ['Meridian', 'Nampa', 'Garden City', 'Kuna', 'Boise', 'Eagle'],
    'IL': ['Alton', 'Lebanon', 'Cottage Hills', 'Maryville', 'Godfrey', 'Cahokia', 'Foster Pond', 'Fairview Heights', 'Madison', 'Mascoutah'],
    'IN': ['Noblesville', 'Indiana', 'Clermont', 'Plainfield', 'Greenwood', 'Lawrence', 'Whiteland', 'Indianapolis', 'Mc Cordsville', 'Martinsville'],
    'LA': ['Gentilly', 'Gretna', 'Bywater', 'Westwego', 'Riveridge', 'Algiers', 'Metairie', 'Marrero', 'Saint Bernard', 'Bucktown'],
    'MO': ['Bel Ridge', 'Creve Coeur', 'Normandy', 'Richmond Heights', 'Green Park', 'Jennings', 'Affton', 'Oakville', 'Winchester', 'University City'],
    'NV': ['Stead', 'Sparks', 'Vc Highlands', 'Sun Valley', 'Virginia City', 'Verdi', 'Spanish Springs', 'Cold Springs', 'Reno', 'Fernley'],
    'PA': ['Eddington', 'Worcester', 'Cedars', 'Thorndale', 'Haverford', 'Saint Peters', 'Wrightstown', 'Solebury', 'Perkasie', 'Tullytown'],
    'TN': ['Pegram', 'Goodlettsville', 'Whites Creek', 'Cane Ridge', 'Mount Juliet', 'White House', 'Joelton', 'Smyrna', 'Berry Hill', 'La Vergne']
}

# URL pública del archivo en GCS
GCS_URL = "https://storage.googleapis.com/modelo-data/df_user_ids.parquet"

# Cargar usuarios desde la URL pública
try:
    df_users = pd.read_parquet(GCS_URL)
    usuarios = ['USUARIO NUEVO'] + df_users["user_id"].head(99).tolist()  # USUARIO NUEVO al inicio de la lista
except Exception as e:
    print(f"Error al cargar los usuarios desde GCS: {e}")
    usuarios = ['USUARIO NUEVO']  # Fallback si ocurre un error
