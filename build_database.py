import os

import sqlite_utils
import datetime
from sqlite_utils.db import NotFoundError
import git
import json


def iterate_file_versions(repo_path, filepath, ref="master"):
    repo = git.Repo(repo_path, odbt=git.GitDB)
    commits = reversed(list(repo.iter_commits(ref, paths=filepath, remove_empty=True)))

    path_parts = os.path.normpath(filepath).split(os.path.sep)
    file_name = path_parts.pop()

    for commit in commits:
        tree = commit.tree
        for p in path_parts:
            tree = commit.tree[p]

        blob = [b for b in tree.blobs if b.name == file_name][0]
        yield commit.committed_datetime, commit.hexsha, blob.data_stream.read()


def create_tables(db):
    db["snapshots"].create(
        {
            "id": int,  # Will be 1, 2, 3 based on order - for nice sorting
            "title": str,  # Human readable date, for display purposes
            "hash": str,
            "when": int,
        },
        pk="id",
    )
    db["snapshots"].create_index(["hash"], unique=True)
    db["crewCurrentStatus"].create({"id": int, "name": str}, pk="id")
    db["cause"].create({"id": int, "name": str}, pk="id")
    db["comments"].create({"id": int, "name": str}, pk="id")
    db["outages"].create(
        {"id": int, "outageStartTime": datetime.datetime, "latitude": str, "longitude": str,}, pk="id",
    )
    db["outage_snapshots"].create(
        {
            "id": str,
            "outage": int,
            "snapshot": int,
            "currentEtor": datetime.datetime,
            "estCustAffected": int,
            "latitude": str,
            "longitude": str,
            "comments": int,
            "cause": int,
            "crewCurrentStatus": int,
        },
        pk="id",
        foreign_keys=("snapshot", "outage", "crewCurrentStatus", "cause", "comments"),
    )


def save_outage(db, outage, when, hash):
    # If outage does not exist, save it first
    outage_id = int(outage["id"])
    try:
        row = db["outages"].get(outage_id)
    except NotFoundError:
        db["outages"].insert(
            {"id": outage_id, "outageStartTime": outage["start_time"], "latitude": outage["lat"], "longitude": outage["lng"],}
        )
    try:
        snapshot_id = list(db["snapshots"].rows_where("hash = ?", [hash]))[0]["id"]
    except IndexError:
        snapshot_id = (
            db["snapshots"].insert({"hash": hash, "title": str(when), "when": int(datetime.datetime.timestamp(when)),}).last_pk
        )
    # Always write an outage_snapshot row
    db["outage_snapshots"].upsert(
        {
            "id": "{}:{}".format(int(datetime.datetime.timestamp(when)), outage_id),
            "outage": outage_id,
            "snapshot": snapshot_id,
            "currentEtor": outage["etr"] if "etr" in outage else None,
            "estCustAffected": int(outage["cust_affected"]) if outage.get("cust_affected") else None,
            "latitude": outage["lat"],
            "longitude": outage["lng"],
            "cause": db["cause"].lookup({"name": outage["cause"]}),
            "comments": db["comments"].lookup({"name": outage["comments"]}),
            "crewCurrentStatus": db["crewCurrentStatus"].lookup({"name": outage["crew_status"]}),
        }
    )


if __name__ == "__main__":
    import sys

    db_name = "lgeku/outages.db"
    db = sqlite_utils.Database(db_name)
    if not db.tables:
        print("Creating tables")
        create_tables(db)
    last_commit_hash = None
    try:
        last_commit_hash = db.conn.execute("select hash from snapshots order by id desc limit 1").fetchall()[0][0]
        ref = "{}..HEAD".format(last_commit_hash)
    except IndexError:
        ref = None
    print("ref =", ref)
    it = iterate_file_versions(".", "lgeku/outages.json", ref)
    count = 0
    for i, (when, hash, outages) in enumerate(it):
        count += 1
        if count % 10 == 0:
            print(count, sep=" ", end=" ")
        for outage in json.loads(outages):
            save_outage(db, outage, when, hash)

    # Materialized view
    with db.conn:
        db.conn.executescript(
            """
DROP TABLE IF EXISTS outages_expanded;
CREATE TABLE outages_expanded (
  outage INT PRIMARY KEY,
  earliest INT,
  latest INT,
  num_snapshots TEXT,
  possible_duration_hours FLOAT,
  probably_ended TEXT,
  min_estCustAffected INT,
  max_estCustAffected INT,
  latitude TEXT,
  longitude TEXT
);
INSERT INTO outages_expanded SELECT
  outage,
  min(snapshots.[when]) as earliest,
  max(snapshots.[when]) as latest,
  json_object("href", "https://pge-outages.simonwillison.net/pge-outages/outage_snapshots?outage=" || outage, "label", count(outage_snapshots.id)) as num_snapshots,
  round(cast(max(snapshots.[when]) - min(snapshots.[when]) as float) / 3600, 2) as possible_duration_hours,
  outage not in (select outage from outage_snapshots) as probably_ended,
  min(outage_snapshots.estCustAffected) as min_estCustAffected,
  max(outage_snapshots.estCustAffected) as max_estCustAffected,
  min(outage_snapshots.latitude) as latitude,
  min(outage_snapshots.longitude) as longitude
from outage_snapshots
  join snapshots on snapshots.id = outage_snapshots.snapshot
group by outage;
        """
        )

    repo = git.Repo(".", odbt=git.GitDB)
    git = repo.git
    git.add(db_name)
    git.commit("-m", f"Updating {db_name}")
