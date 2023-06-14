import json
from typing import TypedDict

from pymongo import MongoClient, database

from analyse import analyse


class Config(TypedDict):
    db_url: str


def read_config() -> Config:
    with open("config.json") as config_file:
        config = json.load(config_file)

    return dict(
        db_url=config["db_url"],
    )


def get_db(db_url: str) -> database.Database:
    client: MongoClient = MongoClient(db_url)
    return client.Stage_database


def main():
    config = read_config()
    db = get_db(config["db_url"])
    analyse(db)


if __name__ == "__main__":
    main()
