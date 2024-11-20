import pandas as pd
import re
import emoji
from nltk.sentiment import SentimentIntensityAnalyzer

# Instancia global de SentimentIntensityAnalyzer
sia = SentimentIntensityAnalyzer()

def preprocess_text(text: str) -> str:
    """Preprocesa el texto (limpieza, emojis). Funciona para texto único o una serie de textos."""
    # Remover todo lo que no sean letras y espacios (mantener caracteres como los acentos)
    text = re.sub(r"[^a-zA-ZáéíóúÁÉÍÓÚñÑ]", " ", str(text))  # Incluye acentos y ñ
    # Reemplazar múltiples espacios por un solo espacio
    text = re.sub(r'\s+', ' ', text).strip()
    # Eliminar URLs, menciones y hashtags
    text = re.sub(r"http\S+|www\S+|@\S+|#\S+", "", text)
    # Convertir a minúsculas
    text = text.lower()
    # Reemplazar los emojis con su descripción textual
    text = emoji.demojize(text)
    return text

def get_sentiment_label(text: str, rating: int = None) -> str:
    """Obtiene el sentimiento del texto usando VADER y ajusta según el rating de estrellas si está disponible."""
    score = sia.polarity_scores(text)['compound']
    
    # Si hay un rating, usamos lógica personalizada basada en los valores
    if rating is not None:
        sentiment_rules = {
            1: 'malo',  # Rating 1 -> siempre malo
            5: 'bueno',  # Rating 5 -> siempre bueno
            2: 'malo' if score < -0.2 else 'neutro',  # Rating 2 -> malo si score < -0.2, else neutro
            3: 'bueno' if score > 0.4 else 'malo' if score < -0.4 else 'neutro', 
            4: 'bueno' if score > 0.2 else 'neutro',  # Rating 4 -> bueno si score > 0.2, else neutro
        }

        return sentiment_rules.get(rating, 'neutro')  # Usar el rating para buscar el sentimiento, por defecto 'neutro'

    # Si no hay rating, usamos el score de VADER
    if score > 0.4:
        return 'bueno' 
    elif score < -0.4:
        return 'malo'   
    return 'neutro'    


def analyze_sentiment(text: str, rating: int = None) -> str:
    """Función principal para analizar el sentimiento del texto (funciona para un solo texto o un DataFrame)."""
    # Preprocesar el texto
    processed_text = preprocess_text(text)
    # Obtener el sentimiento
    sentiment = get_sentiment_label(processed_text, rating)
    return sentiment

def prueba():
    print("importacion correcta de la funcion")

