from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

def enviar_correo_de_prueba():
    # Obtener las variables de entorno de SendGrid
    sendgrid_api_key = os.getenv("SENDGRID_APP_KEY")          # API key de SendGrid
    sendgrid_mail_from = os.getenv("SENDGRID_MAIL_FROM")      # Dirección del remitente
    sendgrid_mail_to = os.getenv("SENDGRID_MAIL_TO")          # Dirección del destinatario (tu correo de prueba)

    # Comprobar si las variables necesarias están configuradas
    if not sendgrid_api_key or not sendgrid_mail_from or not sendgrid_mail_to:
        raise ValueError("Faltan las variables de entorno de SendGrid: revisa API_KEY, MAIL_FROM, y MAIL_TO")

    # Crear el contenido del correo
    message = Mail(
        from_email=sendgrid_mail_from,
        to_emails=sendgrid_mail_to,
        subject="Prueba de integración SendGrid con Airflow",
        html_content="<p>Este es un correo de prueba enviado desde Airflow para verificar la configuración de SendGrid.</p>"
    )

    # Enviar el correo usando SendGrid API
    try:
        sendgrid_client = SendGridAPIClient(sendgrid_api_key)
        response = sendgrid_client.send(message)
        print(f"Correo enviado: {response.status_code}")
    except Exception as e:
        print(f"Error al enviar el correo: {e}")
        raise

# Definir el DAG
with DAG(
    dag_id="dag_prueba_sendgrid",
    start_date=datetime(2024, 10, 31),
    schedule_interval=None,  # Ejecutar manualmente
    catchup=False,
) as dag:
    # Definir la tarea de envío de correo
    enviar_correo_task = PythonOperator(
        task_id="enviar_correo_de_prueba",
        python_callable=enviar_correo_de_prueba,
    )
