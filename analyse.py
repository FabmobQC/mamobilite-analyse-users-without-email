from pymongo import database

collections_names = [
    "Stage_Profiles",
    "Stage_analysis_timeseries",
    "Stage_pipeline_state",
    "Stage_timeseries",
    "Stage_timeseries_error",
    "Stage_usercache",
    "Stage_uuids",
]


def get_bsonsize_collection(db: database.Database, collection_name: str):
    """_summary_
    Get the bsonsize of a collection
    """
    aggregation_cursor = db[collection_name].aggregate(
        [{"$group": {"_id": None, "size": {"$sum": {"$bsonSize": "$$ROOT"}}}}]
    )
    try:
        return aggregation_cursor.next()["size"]
    except StopIteration:
        return 0


def get_bsonsize_without_email(db: database.Database, collection_name: str):
    """
    Get the bsonsize of all documents within a collection for which the user has no email 
    """
    total = 0
    # Attemps to do it in one single aggregation resulted in too large document
    users_cursor = db.Stage_Profiles.find({"email": None}, {"user_id": 1})
    for user in users_cursor:
        aggregation_cursor = db[collection_name].aggregate(
            [
                {"$match": {"user_id": user["user_id"]}},
                {"$group": {"_id": None, "size": {"$sum": {"$bsonSize": "$$ROOT"}}}},
            ]
        )
        try:
            total += aggregation_cursor.next()["size"]
        except StopIteration:
            pass
    return total


def get_bsonsize_without_user(db: database.Database, collection_name: str):
    """
    Get the bsonsize of all documents within a collection for which the user_id is ""
    """
    aggregation_cursor = db[collection_name].aggregate(
        [
            {"$match": {"user_id": ""}},
            {"$group": {"_id": None, "size": {"$sum": {"$bsonSize": "$$ROOT"}}}},
        ]
    )
    try:
        return aggregation_cursor.next()["size"]
    except StopIteration:
        return 0


def analyse(db: database.Database):
    total = 0
    total_without_email = 0
    total_without_user = 0
    for collection_name in collections_names:
        print(collection_name)

        # bsonsize of the collection
        bsonsize_collection = get_bsonsize_collection(db, collection_name)
        total += bsonsize_collection
        print(f"\ttotal: {bsonsize_collection}")

        # bsonsize of users without email
        bsonsize_without_email = get_bsonsize_without_email(db, collection_name)
        percent_without_email = bsonsize_without_email / bsonsize_collection * 100
        total_without_email += bsonsize_without_email
        print(f"\twithout email: {bsonsize_without_email } ({percent_without_email }%)")

        # bsonsize of data without user
        bsonsize_without_user = get_bsonsize_without_user(db, collection_name)
        percent_without_user = bsonsize_without_user / bsonsize_collection * 100
        total_without_user += bsonsize_without_user
        print(f"\twithout user: {bsonsize_without_user} ({percent_without_user}%)")

        # bsonsize that could be freed
        freeable = bsonsize_without_email + bsonsize_without_user
        percent_freeable = freeable / bsonsize_collection * 100
        print(f"\tcould be freed: {freeable} ({percent_freeable}%)")

    print("\nStage_database")
    print(f"\ttotal: {total}")
    percent_total_without_email = total_without_email / total * 100
    print(f"\twithout email: {total_without_email } ({percent_total_without_email }%)")
    percent_total_without_user = total_without_user / total * 100
    print(f"\twithout user: {total_without_user} ({percent_total_without_user}%)")
    total_freeable = total_without_email + total_without_user
    percent_total_freeable = total_freeable / total * 100
    print(f"\tcould be freed: {total_freeable} ({percent_total_freeable}%)")
