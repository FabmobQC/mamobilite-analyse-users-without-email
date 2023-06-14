from pymongo import database

from utils import get_users_without_email


def analyse(db: database.Database):
    get_users_without_email(db)
