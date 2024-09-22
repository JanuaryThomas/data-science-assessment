import boto3
import json


def get_secrets(name: str) -> dict:
    """[summary]
    The secretes from AWS secret manager

    Args:
        name (str): name of secret

    Returns:
        [dict]: dictionary of secret details
    """
    client = boto3.client("secretsmanager", region_name="eu-west-1")
    try:
        response = client.get_secret_value(SecretId=name)
        secrets = json.loads(response["SecretString"])
        return secrets
    except Exception as error:
        return None
