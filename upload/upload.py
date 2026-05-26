

"""
========================================================
Databricks Volume Upload — Bronze Platform
========================================================

User provides ONLY:
    Volume Name

Automatically reads:
    workspace_url
    token
    catalog
    schema

From:
    session_output/session_config.json

Uploads:
    latest migration_plan.json → Unity Catalog Volume
========================================================
"""

import json
import os
import glob
import requests
import sys


# ---------------------------------------------------
# LOAD SESSION CONFIG
# ---------------------------------------------------

def load_config():

    config_path = "streaming data/session_output/session_config.json"

    if not os.path.exists(config_path):
        print("❌ session_config.json not found")
        sys.exit(1)

    with open(config_path, "r") as f:
        cfg = json.load(f)

    workspace = cfg["databricks_workspace_url"].rstrip("/")
    token = cfg["databricks_token"]
    catalog = cfg["catalog"]
    schema = cfg["schema"]

    return workspace, token, catalog, schema


# ---------------------------------------------------
# FIND LATEST MIGRATION PLAN
# ---------------------------------------------------

def find_latest_plan():

    pattern = "streaming data/session_output/run_*/migration_plan.json"

    files = sorted(glob.glob(pattern))

    if not files:
        print("❌ migration_plan.json not found")
        sys.exit(1)

    return files[-1]


# ---------------------------------------------------
# CREATE VOLUME IF NOT EXISTS
# ---------------------------------------------------

def ensure_volume(workspace, token, catalog, schema, volume):

    url = f"{workspace}/api/2.1/unity-catalog/volumes"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    r = requests.get(
        url,
        headers=headers,
        params={"catalog_name": catalog, "schema_name": schema}
    )

    if r.status_code == 200:

        volumes = [v["name"] for v in r.json().get("volumes", [])]

        if volume in volumes:
            print("✔ Volume already exists")
            return

    payload = {
        "catalog_name": catalog,
        "schema_name": schema,
        "name": volume,
        "volume_type": "MANAGED"
    }

    r = requests.post(url, headers=headers, json=payload)

    if r.status_code not in [200, 201]:
        print("❌ Failed to create volume")
        print(r.text)
        sys.exit(1)

    print("✔ Volume created")


# ---------------------------------------------------
# UPLOAD FILE
# ---------------------------------------------------

def upload_file(workspace, token, catalog, schema, volume, plan):

    filename = os.path.basename(plan)

    volume_path = f"/Volumes/{catalog}/{schema}/{volume}/{filename}"

    url = f"{workspace}/api/2.0/fs/files{volume_path}"

    headers = {
        "Authorization": f"Bearer {token}"
    }

    with open(plan, "rb") as f:
        data = f.read()

    r = requests.put(url, headers=headers, data=data)

    if r.status_code in [200, 201, 204]:

        print("\n✅ Upload successful")
        print("Volume Path:")
        print(volume_path)

        return volume_path

    print("\n❌ Upload failed")
    print(r.text)
    sys.exit(1)


# ---------------------------------------------------
# MAIN
# ---------------------------------------------------

def main():

    print("\n===================================")
    print(" BRONZE PLATFORM — VOLUME UPLOAD")
    print("===================================\n")

    workspace, token, catalog, schema = load_config()

    print(f"Catalog : {catalog}")
    print(f"Schema  : {schema}\n")

    volume = input("Enter Volume Name: ").strip()

    plan = find_latest_plan()

    print("\nMigration Plan Found:")
    print(plan)

    ensure_volume(workspace, token, catalog, schema, volume)

    path = upload_file(workspace, token, catalog, schema, volume, plan)

    print("\n-----------------------------------")
    print("Use this in execution.py")
    print(f'CONFIG = "{path}"')
    print("-----------------------------------\n")


if __name__ == "__main__":
    main()