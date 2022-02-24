from chessdotcom import get_player_game_archives
from io import StringIO
from prefect import task, flow
from typing import Tuple
import chess.pgn as pgn
import pandas as pd
import requests

class Game(pgn.Game):
    def __init__(self: object, pgn_str: str, uuid: str):
        self.game_obj = pgn.read_game(StringIO(pgn_str)).__dict__
        self.uuid = uuid
        self.game_obj['variations'] = str(self.game_obj['variations'][0])

    def to_df(self: object) -> pd.DataFrame:
        df = pd.json_normalize(dict(self.game_obj['headers']))
        df['pgn'] = self.game_obj['variations']
        return df

@task
def get_games(url: str) -> Tuple[Game]:    
    month_of_games = requests.get(url).json()['games']
    return (Game(game['pgn'], game['uuid']) for game in month_of_games)

@task
def load_games(games: Tuple[Game], base_path: str, write_local=False) -> None:
    df = pd.concat([game.to_df() for game in games])
    year, month, _ = list(df['Date'])[0].split('.')

    if write_local: 
        df.to_csv(f"{base_path}/archive/{month}-{year}.csv", index=False)
    else:
        filepath = f's3://{base_path}/archived-chess-games/{year}/{month}.parquet.gzip'
        df.to_parquet(filepath, compression='gzip')

@flow
def store_user(username: str, storage: str) -> None:
    archive_urls = get_player_game_archives(username=username).archives
    # pass endpoints for a month worth of games: List[str]
    for month_of_games in [get_games(url) for url in archive_urls]:
        load_games(games=month_of_games, base_path=f"{storage}/{username}")    

@flow
def orca(S3_bucket='nate-demo-bucket') -> None:
    for username in ['n80n8', 'HowellV']:
        print(f'Finding games from {username}..')
        store_user(username, S3_bucket)

if __name__ == "__main__":
    orca()