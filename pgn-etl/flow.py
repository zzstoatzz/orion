from chessdotcom import get_player_game_archives
from datetime import timedelta
from io import StringIO
from prefect import flow, task
from prefect.deployments import DeploymentSpec
from prefect.orion.schemas.schedules import IntervalSchedule
from prefect.tasks import task_input_hash
from prefect.task_runners import DaskTaskRunner
from typing import Tuple
import boto3, chess.pgn as pgn, pandas as pd, requests

class Game(pgn.Game):
    def __init__(self: object, pgn_str: str):
        self.game_obj = pgn.read_game(StringIO(pgn_str)).__dict__
        self.game_obj['variations'] = str(self.game_obj['variations'][0])
        self.df = pd.json_normalize(dict(self.game_obj['headers']))
        self.df['pgn'] = self.game_obj['variations']

def alreadyStored(url: str, bucket: str, username: str) -> bool:
    client = boto3.client('s3')
    year, month = url.split('/')[-2:]
    archive_date = f"{year}/{month}"
    stored_months = [i['Key'].split('.')[0] for i in client.list_objects_v2(Bucket=bucket, Prefix=username)['Contents']]
    return any([archive_date in i for i in stored_months])


@task(cache_key_fn=task_input_hash)
def get_games(url: str) -> Tuple[Game]:
    print(f"GET {url}")
    raw_games = requests.get(url).json()['games']
    return (Game(game['pgn']) for game in raw_games)

@task
def load_games(games: Tuple[Game], base_path: str) -> None:
    df = pd.concat([game.df for game in games])
    year, month, _ = list(df['Date'])[0].split('.')
    print(f'storing games from month {month} of year {year}...')
    filepath = f's3://{base_path}/archived-chess-games/{year}/{month}.parquet.gzip'
    df.to_parquet(filepath, compression='gzip')

@flow(name="Store chess.com users' games", task_runner=DaskTaskRunner(), version='0.0.1')
def orca(S3_bucket: str) -> None:
    for username in ['HowellV']:

        archive_urls = get_player_game_archives(username=username).archives

        user_games = [get_games(url) for url in archive_urls if not alreadyStored(url, S3_bucket, username) or True]

        if len(user_games) == 0:
            print(f'No new months of games to load for {username}!')
            return

        print(f'Fetching {len(user_games) } new months of games from {username}..')

        for month in user_games:
            load_games(
                games=month,
                base_path=f"{S3_bucket}/{username}"
            )

DeploymentSpec(
    flow_location='flow.py',
    name='PGN ETL',
    flow_name="Store chess.com users' games",
    schedule=IntervalSchedule(interval=timedelta(days=1)),
    parameters={"S3_bucket": "nate-demo-bucket"},
    tags=["chess", "funsies"]
)
if __name__ == "__main__":
    orca('nate-demo-bucket')