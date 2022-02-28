from chessdotcom import get_player_game_archives
from datetime import datetime, timedelta
from io import StringIO
from prefect import flow, task
from prefect.deployments import DeploymentSpec
from prefect.orion.schemas.schedules import IntervalSchedule
from prefect.task_runners import DaskTaskRunner
from typing import Tuple
import boto3, chess.pgn as pgn, pandas as pd, requests

class Game(pgn.Game):
    def __init__(self: object, pgn_str: str, uuid: str):
        self.id = uuid
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

@task
def get_games(url: str) -> Tuple[Game]:    
    raw_games = requests.get(url).json()['games']
    return (Game(game['pgn'], game['uuid']) for game in raw_games)

@task
def load_games(games: Tuple[Game], base_path: str, write_local: bool=False) -> None:
    df = pd.concat([game.df for game in games])
    year, month, _ = list(df['Date'])[0].split('.')
    print(f'storing games from month {month} of year {year}...')

    if write_local: 
        df.to_csv(f"/Users/nate/Downloads/archive/{month}-{year}.csv", index=False)
    else:
        filepath = f's3://{base_path}/archived-chess-games/{year}/{month}.parquet.gzip'
        df.to_parquet(filepath, compression='gzip')
    

@flow(name="Store a user")
def store_user(username: str, storage: str) -> None:
    archive_urls = get_player_game_archives(username=username).archives

    # own task?
    new_games = [get_games(month) for month in archive_urls if not alreadyStored(month, storage, username)]

    if len(new_games) == 0:
        print(f'No new months of games to store for {username}..')
    else:
        print(f'Found {len(new_games)} months of games to store for {username}')
        for month_of_games in new_games:
            load_games(games=month_of_games, base_path=f"{storage}/{username}", write_local=True)    

@flow(name="do things including Store a user", task_runner=DaskTaskRunner(), version='0.0.1')
def orca(S3_bucket: str) -> None:
    for username in ['HowellV']:
        print(f'Finding games from {username}..')
        store_user(username, S3_bucket)

DeploymentSpec(
    flow_location='client.py',
    name='PGN ETL',
    flow_name='do things including Store a user',
    # schedule=IntervalSchedule(interval=timedelta(days=1)),
    parameters={"S3_bucket": "nate-demo-bucket"},
    tags=["chess", "funsies"]
)
if __name__ == "__main__":
    orca('nate-demo-bucket')

