from prefect import flow, task

import requests


@task(name='Get Entity')
def get(endpoint: str, access_token: str = 'cvsUFI4_V-dJYDAGRsZC') -> dict:

    headers = {
        'Authorization': f'Bearer {access_token}'
    }

    response = requests.get(
        url = f'https://the-one-api.dev/v2/{endpoint}',
        headers=headers
    )

    if response.ok:
        match response.json():
            case {'docs': movies}:
                print(*[f"{movie['name']}\n" for movie in movies])

@flow
def something():
    get('book')

if __name__ == "__main__":
    something()