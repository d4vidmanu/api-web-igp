import requests
import boto3
import uuid
from datetime import datetime

def lambda_handler(event, context):
    # URL de la API que contiene los datos de los sismos
    api_url = "https://ultimosismo.igp.gob.pe/api/ultimo-sismo/ajaxb/2024"
    
    # Hacer la solicitud HTTP a la API
    response = requests.get(api_url)
    if response.status_code != 200:
        return {
            'statusCode': response.status_code,
            'body': 'Error al acceder a la API de sismos'
        }

    # Procesar la respuesta JSON
    sismos = response.json()

    # Filtrar los sismos de noviembre
    sismos_noviembre = []
    for sismo in sismos:
        try:
            fecha_local = datetime.strptime(sismo["fecha_local"], "%Y-%m-%dT%H:%M:%S.%fZ")
            if fecha_local.month == 11:  # Filtrar solo noviembre
                sismos_noviembre.append(sismo)
        except Exception as e:
            continue  # Ignorar errores de formato en la fecha

    # Guardar los datos en DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('TablaWebScrapping')  # Reemplazar con el nombre real de tu tabla

    # Eliminar todos los elementos existentes
    scan = table.scan()
    with table.batch_writer() as batch:
        for item in scan['Items']:
            batch.delete_item(Key={'id': item['id']})

    # Insertar los nuevos datos
    for sismo in sismos_noviembre:
        sismo['id'] = str(uuid.uuid4())  # Generar un ID único para DynamoDB
        table.put_item(Item=sismo)

    return {
        'statusCode': 200,
        'body': sismos_noviembre  # Retornar los datos filtrados
    }
