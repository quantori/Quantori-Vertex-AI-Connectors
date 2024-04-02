from google.cloud import secretmanager


def access_secret_version(secret: str) -> str:
    client = secretmanager.SecretManagerServiceClient()
    response = client.access_secret_version(request={"name": secret})

    payload = response.payload.data.decode("UTF-8")
    return payload
