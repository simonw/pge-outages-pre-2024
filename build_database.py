import sqlite_utils
import datetime
from sqlite_utils.db import NotFoundError
import git
import json


def iterate_file_versions(repo_path, filepath, ref="master"):
    repo = git.Repo(repo_path, odbt=git.GitDB)
    commits = reversed(list(repo.iter_commits(ref, paths=filepath)))
    for commit in commits:
        blob = [b for b in commit.tree.blobs if b.name == filepath][0]
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
    db["regionName"].create({"id": int, "name": str}, pk="id")
    db["crewCurrentStatus"].create({"id": int, "name": str}, pk="id")
    db["cause"].create({"id": int, "name": str}, pk="id")
    db["outages"].create(
        {
            "id": int,
            "outageStartTime": int,
            "latitude": str,
            "longitude": str,
            "regionName": int,
            "outageDevices": str,
        },
        pk="id",
        foreign_keys=("regionName",),
    )
    db["outage_snapshots"].create(
        {
            "id": str,
            "outage": int,
            "snapshot": int,
            "lastUpdateTime": int,
            "currentEtor": int,
            "autoEtor": int,
            "estCustAffected": int,
            "hazardFlag": int,
            "latitude": str,
            "longitude": str,
            "regionName": int,
            "cause": int,
            "crewCurrentStatus": int,
        },
        pk="id",
        foreign_keys=("regionName", "snapshot", "outage", "crewCurrentStatus", "cause"),
    )


def save_outage(db, outage, when, hash):
    # If outage does not exist, save it first
    outage_id = int(outage["outageNumber"])
    try:
        row = db["outages"].get(outage_id)
    except NotFoundError:
        db["outages"].insert(
            {
                "id": outage_id,
                "outageStartTime": int(outage["outageStartTime"]),
                "latitude": outage["latitude"],
                "longitude": outage["longitude"],
                "regionName": db["regionName"].lookup({"name": outage["regionName"]}),
                "outageDevices": outage.get("outageDevices") or [],
            }
        )
    try:
        snapshot_id = list(db["snapshots"].rows_where("hash = ?", [hash]))[0]["id"]
    except IndexError:
        snapshot_id = (
            db["snapshots"]
            .insert(
                {
                    "hash": hash,
                    "title": str(when),
                    "when": int(datetime.datetime.timestamp(when)),
                }
            )
            .last_pk
        )
    # Always write an outage_snapshot row
    db["outage_snapshots"].upsert(
        {
            "id": "{}:{}".format(int(datetime.datetime.timestamp(when)), outage_id),
            "outage": outage_id,
            "snapshot": snapshot_id,
            "lastUpdateTime": int(outage["lastUpdateTime"])
            if "lastUpdateTime" in outage
            else None,
            "currentEtor": int(outage["currentEtor"])
            if "currentEtor" in outage
            else None,
            "autoEtor": int(outage["autoEtor"]) if "autoEtor" in outage else None,
            "estCustAffected": int(outage["estCustAffected"])
            if outage.get("estCustAffected")
            else None,
            "hazardFlag": int(outage["hazardFlag"]),
            "latitude": outage["latitude"],
            "longitude": outage["longitude"],
            "regionName": db["regionName"].lookup({"name": outage["regionName"]}),
            "cause": db["cause"].lookup({"name": outage["cause"]}),
            "crewCurrentStatus": db["crewCurrentStatus"].lookup(
                {"name": outage["crewCurrentStatus"]}
            ),
        }
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
        last_commit_hash = db.conn.execute(
            "select hash from snapshots order by id desc limit 1"
        ).fetchall()[0][0]
        ref = "{}..HEAD".format(last_commit_hash)
    except IndexError:
        ref = None
    print("ref =", ref)
    it = iterate_file_versions(".", "pge-outages.json", ref)
    count = 0
    for i, (when, hash, outages) in enumerate(it):
        count += 1
        if count % 10 == 0:
            print(count, sep=" ", end=" ")
        for outage in json.loads(outages):
            save_outage(db, outage, when, hash)

    # Materialized view
    with db.conn:
        db.conn.executescript("""
DROP TABLE IF EXISTS outages_expanded;
CREATE TABLE outages_expanded (
  outage INT PRIMARY KEY,
  earliest INT,
  latest INT,
  possible_duration_hours FLOAT,
  probably_ended TEXT,
  min_estCustAffected INT,
  max_estCustAffected INT,
  region TEXT,
  latitude TEXT,
  longitude TEXT
);
INSERT INTO outages_expanded SELECT
  outage,
  min(snapshots.[when]) as earliest,
  max(snapshots.[when]) as latest,
  json_object("href", "https://pge-outages.simonwillison.net/pge-outages/outage_snapshots?outage=" || outage, "label", count(outage_snapshots.id)) as num_snapshots,
  round(cast(max(snapshots.[when]) - min(snapshots.[when]) as float) / 3600, 2) as possible_duration_hours,
  outage not in (select outage from most_recent_snapshot) as probably_ended,
  min(outage_snapshots.estCustAffected) as min_estCustAffected,
  max(outage_snapshots.estCustAffected) as max_estCustAffected,
  min(regionName.name) as region,
  min(outage_snapshots.latitude) as latitude,
  min(outage_snapshots.longitude) as longitude
from outage_snapshots
  join snapshots on snapshots.id = outage_snapshots.snapshot
  join regionName on outage_snapshots.regionName = regionName.id
group by outage;
        """)