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
        {"id": str, "outageStartTime": int, "latitude": str, "longitude": str}, pk="id",
    )
    db["outage_snapshots"].create(
        {
            "id": str,
            "outage": int,
            "snapshot": int,
            "cluster": bool,
            "currentEtor": int,
            "estCustAffected": int,
            "latitude": str,
            "longitude": str,
            "comments": int,
            "cause": int,
            "crewCurrentStatus": int,
            "source": str,
        },
        pk="id",
        foreign_keys=("snapshot", "outage", "crewCurrentStatus", "cause", "comments"),
    )


def save_outage(db, outage, when, hash):
    # If outage does not exist, save it first
    outage_id = outage["id"]
    try:
        row = db["outages"].get(outage_id)
    except NotFoundError:
        db["outages"].insert(
            {
                "id": outage_id,
                "outageStartTime": outage["startTime"],
                "latitude": outage["latitude"],
                "longitude": outage["longitude"],
            }
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
            "cluster": outage.get("cluster", False),
            "currentEtor": int(datetime.datetime.strptime(outage["etr"], "%Y-%m-%dT%H:%M:%S%z").timestamp())
            if "etr" and outage["etr"] != "ETR-NULL" in outage
            else None,
            "estCustAffected": int(outage["custAffected"]) if outage.get("custAffected") else None,
            "latitude": outage["latitude"],
            "longitude": outage["longitude"],
            "cause": db["cause"].lookup({"name": outage["cause"]}),
            "comments": db["comments"].lookup({"name": outage["comments"]}),
            "crewCurrentStatus": db["crewCurrentStatus"].lookup({"name": outage["crew_status"]})
            if outage.get("crew_status")
            else None,
            "source": outage["source"],
        }, pk="id"
    )


if __name__ == "__main__":
    import sys

    db_name = sys.argv[-1]
    assert db_name.endswith(".db")
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
DROP VIEW IF EXISTS most_recent_snapshot;
CREATE VIEW most_recent_snapshot as
select
    outage_snapshots.id, latitude, longitude, outage, estCustAffected, cluster,
    cause.name as cause, crewCurrentStatus.name as crewCurrentStatus, comments.name as comments,
    'https://lgeku-outages.herokuapp.com/outages/outages/' || outage as outage_url, currentEtor
from outage_snapshots
    left join cause on outage_snapshots.cause = cause.id
    left join comments on outage_snapshots.comments = comments.id
    left join crewCurrentStatus on outage_snapshots.crewCurrentStatus = crewCurrentStatus.id
where
    snapshot in (select id from snapshots order by id desc limit 1);   
             
DROP TABLE IF EXISTS outages_expanded;
CREATE TABLE outages_expanded (
  outage STR PRIMARY KEY,
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
  json_object("href", "https://lgeku-outages.herokuapp.com/outages/outage_snapshots?outage=" || outage, "label", count(outage_snapshots.id)) as num_snapshots,
  round(cast(max(snapshots.[when]) - min(snapshots.[when]) as float) / 3600, 2) as possible_duration_hours,
  outage not in (select outage from most_recent_snapshot) as probably_ended,
  min(outage_snapshots.estCustAffected) as min_estCustAffected,
  max(outage_snapshots.estCustAffected) as max_estCustAffected,
  min(outage_snapshots.latitude) as latitude,
  min(outage_snapshots.longitude) as longitude
from outage_snapshots
  join snapshots on snapshots.id = outage_snapshots.snapshot
group by outage;
        """
        )
