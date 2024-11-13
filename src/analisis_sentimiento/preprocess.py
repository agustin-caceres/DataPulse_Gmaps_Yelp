import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
from nltk import pos_tag
import re

# Configurar lematizador y stopwords
lemmatizer = WordNetLemmatizer()
stop_words = set(stopwords.words('english'))

# Función para limpiar y lematizar el texto
def preprocess_text(text:str) -> str:
    '''
    Preprocesa una cadena de texto utilizando expresiones regulares, tokenizacion, eliminacion de nombres y lemmatizacion. Optimizando las palabras importantes.
    
    Parameters:
    ----------
    text (str):
        El texto a procesar.
    
    Return (str): 
    ----------
        El texto limpio y procesado.
    '''
    # Sacamos todo lo que no sean letras.
    text = re.sub("[^a-zA-Z]"," ",str(text))
    # Reemplaza múltiples espacios por un solo espacio
    text = re.sub(r'\s+', ' ', text).strip()
    # Convertir a minúsculas
    text = text.lower() 
    
    # Tokenizar el texto
    tokens = word_tokenize(text)
    
    # Etiquetar las palabras
    tag_tokens = pos_tag(tokens)
    # Filtrar nombres propios (NNP, NNPS)
    tokens = [word for word, tag in tag_tokens if tag not in ['NNP', 'NNPS']]
    # Convertir el resto del texto a minúsculas. Esto debe ser luego de verificar nombres propios con Capitalizacion.
    tokens = [word.lower() for word in tokens]
    
    # Eliminar stopwords
    tokens = [word for word in tokens if word not in stop_words]
    
    # Lematizar las palabras
    tokens = [lemmatizer.lemmatize(word) for word in tokens]
    
    return ' '.join(tokens)