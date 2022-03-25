from chessdotcom import get_player_game_archives
from io import StringIO
from typing import Generator, List, Tuple
import boto3, chess.pgn as pgn, pandas as pd, requests

# https://python-chess.readthedocs.io/en/latest/pgn.html
class Game(pgn.Game):
    def __init__(self: object, pgn_str: str):
        self.game_obj = pgn.read_game(StringIO(pgn_str)).__dict__
        self.game_obj['variations'] = str(self.game_obj['variations'][0])
        self.df = pd.json_normalize(dict(self.game_obj['headers']))
        self.df['pgn'] = self.game_obj['variations']

def alreadyStored(bucket: str, username: str) -> List[str]:
    client = boto3.client('s3')
    try:
        return [i['Key'].split('.')[0].lower() for i in client.list_objects_v2(Bucket=bucket, Prefix=username)['Contents']]
    except KeyError:
        return []

def get_games(url: str) -> Generator[Game, None, None]:
    print(f"GET {url}")
    for game in requests.get(url).json()['games']:
        yield Game(game['pgn'])

def load_games(games: Tuple[Game], base_path: str) -> None:
    df = pd.concat([game.df for game in games])
    year, month, _ = list(df['Date'])[0].split('.')
    print(f'storing games from month {month} of year {year}...')
    filepath = f's3://{base_path}/games/{year}/{month}.parquet.gzip'
    df.to_parquet(filepath, compression='gzip')

def orca(S3_bucket: str) -> None:

    for username in ['jamessopkin']:
        print(f"Checking for games on chess.com from: {username}")

        # list of URLs to GET months of games from
        archive_urls = get_player_game_archives(username=username).archives

        # only get_games if they're not already stored
        stored_games = alreadyStored(S3_bucket, username)
        path_from = lambda url: url.split('player/')[-1]
        new_user_games = [get_games(url) for url in archive_urls if path_from(url) not in stored_games]

        if len(new_user_games) == 0:
            print(f'No new months of games to load for {username}!')
            continue

        print(f'Fetching {len(new_user_games) } new months of games from {username}..')

        for month in new_user_games:
            load_games(
                games=month,
                base_path=f"{S3_bucket}/{username}"
            )

if __name__ == "__main__":
    orca('nate-demo-bucket')