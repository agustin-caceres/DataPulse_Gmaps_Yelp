from fastapi import FastAPI
import joblib

model = joblib.load('model/model.pkl')

app = FastAPI()


@app.get("/")
async def read_root():
    return {
        "Message": "DATA PULSE Analytics...",
        "Documentacion": "/docs",
    }


@app.get_id("/recomendation")
async def get_id(id: str) -> list[dict]:
    """get_id: Recibe el `id` de un Usuario y devuelve una lista
               con los restaurantes a recomendar

    Args:
        id (str): `id` del usuario

    Returns:
        list: Lista de Restaurantes a recomendar
    """
    pass

    """ACA donde encuentra las recomendaciones dentro del modelo
       y lo agrega todo a una lista de diccionarios y por ultimo retorna
       return list_recomendations"""
