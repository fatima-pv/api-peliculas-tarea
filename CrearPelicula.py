import boto3
import uuid
import os
import json

def lambda_handler(event, context):
    try:
        # Log de entrada
        log_entrada = {
            "tipo": "INFO",
            "log_datos": {
                "mensaje": "Inicio de procesamiento",
                "event": event
            }
        }
        print(json.dumps(log_entrada))
        
        # Entrada (json)
        tenant_id = event['body']['tenant_id']
        pelicula_datos = event['body']['pelicula_datos']
        nombre_tabla = os.environ["TABLE_NAME"]
        
        # Proceso
        uuidv4 = str(uuid.uuid4())
        pelicula = {
            'tenant_id': tenant_id,
            'uuid': uuidv4,
            'pelicula_datos': pelicula_datos
        }
        
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(nombre_tabla)
        response = table.put_item(Item=pelicula)
        
        # Log de éxito
        log_exito = {
            "tipo": "INFO",
            "log_datos": {
                "mensaje": "Película creada exitosamente",
                "tenant_id": tenant_id,
                "uuid": uuidv4,
                "pelicula_datos": pelicula_datos,
                "dynamodb_response": {
                    "status": response['ResponseMetadata']['HTTPStatusCode']
                }
            }
        }
        print(json.dumps(log_exito))
        
        # Salida (json)
        return {
            'statusCode': 200,
            'pelicula': pelicula,
            'response': response
        }
        
    except KeyError as e:
        # Error de clave faltante
        log_error = {
            "tipo": "ERROR",
            "log_datos": {
                "mensaje": "Error: Campo requerido faltante",
                "error_type": "KeyError",
                "error_detail": str(e),
                "event": event
            }
        }
        print(json.dumps(log_error))
        
        return {
            'statusCode': 400,
            'error': f'Campo requerido faltante: {str(e)}'
        }
        
    except Exception as e:
        # Error general
        log_error = {
            "tipo": "ERROR",
            "log_datos": {
                "mensaje": "Error inesperado en el procesamiento",
                "error_type": type(e).__name__,
                "error_detail": str(e),
                "event": event
            }
        }
        print(json.dumps(log_error))
        
        return {
            'statusCode': 500,
            'error': f'Error interno: {str(e)}'
        }
