"""nas_cleanup_neo4j_data.py

Utility helpers to safely clean up Neo4j data in batches.

Features:
- connect to Neo4j using the official `neo4j` python driver (listed in requirements)
- dry-run counts
- batched DETACH DELETE by label and optional WHERE clause
- batched delete of orphan nodes
- remove properties in batches
- small CLI for common actions

Usage examples (from shell):
  python nas_cleanup_neo4j_data.py --uri bolt://localhost:7687 --user neo4j --password pass dry-count --label OldLabel
  python nas_cleanup_neo4j_data.py --uri bolt://localhost:7687 --user neo4j --password pass delete-label --label OldLabel --batch-size 500

Be conservative: run dry-count first and back up your DB before destructive actions.
"""
from __future__ import annotations

import re
import argparse
import logging
from typing import Optional, Dict, Any

# from neo4j import GraphDatabase, Driver
import clean_metadata
from neo4j_config import driver, close_driver

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def sanitize_label(label: str) -> str:
    """Allow only alphanumeric and underscore labels to avoid injection.

    Raises ValueError if label contains invalid characters.
    """
    if not re.match(r"^[A-Za-z0-9_]+$", label):
        raise ValueError("Label contains invalid characters. Allowed: A-Z a-z 0-9 _")
    return label


# def get_driver(uri: str, user: str, password: str) -> Driver:
#     return GraphDatabase.driver(uri, auth=(user, password))


def count_nodes( label: Optional[str] = None, where_cypher: Optional[str] = None, params: Optional[Dict[str, Any]] = None) -> int:
    """Return the number of nodes matching the optional label and where clause."""
    if label:
        label = sanitize_label(label)
        label_clause = f":{label}"
    else:
        label_clause = ""
    where_clause = f"WHERE {where_cypher}" if where_cypher else ""
    query = f"MATCH (n{label_clause}) {where_clause} RETURN count(n) AS cnt"
    with driver.session() as session:
        rec = session.run(query, **(params or {})).single()
        return int(rec["cnt"]) if rec else 0


def delete_nodes_by_label_batch(label: str, where_cypher: Optional[str] = None, params: Optional[Dict[str, Any]] = None, batch_size: int = 1000) -> int:
    """Delete nodes with a label (and optional WHERE clause) in batches.

    Returns total deleted.
    """
    label = sanitize_label(label)
    total_deleted = 0
    where_clause = f"WHERE {where_cypher}" if where_cypher else ""
    query = f"MATCH (n:{label}) {where_clause} WITH n LIMIT $limit DETACH DELETE n RETURN count(n) AS deleted"
    params = dict(params or {})

    while True:
        with driver.session() as session:
            rec = session.run(query, **{**params, "limit": batch_size}).single()
            deleted = int(rec["deleted"]) if rec else 0
        total_deleted += deleted
        logger.info("Deleted %d nodes in this batch (total %d)", deleted, total_deleted)
        if deleted == 0:
            break
    return total_deleted


def delete_orphan_nodes_batch(batch_size: int = 1000) -> int:
    """Delete nodes with no relationships in batches. Returns total deleted."""
    total_deleted = 0
    query = (
        "MATCH (n) WHERE size((n)--()) = 0 WITH n LIMIT $limit DETACH DELETE n RETURN count(n) AS deleted"
    )
    while True:
        with driver.session() as session:
            rec = session.run(query, limit=batch_size).single()
            deleted = int(rec["deleted"]) if rec else 0
        total_deleted += deleted
        logger.info("Deleted %d orphan nodes in this batch (total %d)", deleted, total_deleted)
        if deleted == 0:
            break
    return total_deleted


def remove_property_batch(label: str, prop: str, batch_size: int = 1000) -> int:
    """Remove a property from nodes of a label in batches. Returns total nodes updated.

    Note: This sets the property to NULL which effectively removes it for Neo4j.
    """
    label = sanitize_label(label)
    total = 0
    query = (
        f"MATCH (n:{label}) WHERE exists(n.{prop}) WITH n LIMIT $limit SET n.{prop} = NULL RETURN count(n) AS updated"
    )
    while True:
        with driver.session() as session:
            rec = session.run(query, limit=batch_size).single()
            updated = int(rec["updated"]) if rec else 0
        total += updated
        logger.info("Cleared property '%s' on %d nodes in this batch (total %d)", prop, updated, total)
        if updated == 0:
            break
    return total


def drop_constraint(name: str) -> None:
    q = f"DROP CONSTRAINT {name} IF EXISTS"
    with driver.session() as session:
        session.run(q)
    logger.info("Dropped constraint (if existed): %s", name)

def drop_index(name: str) -> None:
    q = f"DROP INDEX {name} IF EXISTS"
    with driver.session() as session:
        session.run(q)
    logger.info("Dropped index (if existed): %s", name)

# def try_load_repo_config() -> Optional[Dict[str, str]]:
#     """Attempt to load `neo4j_config.py` from the repo root or `nas/`.

#     It's optional helper to pre-fill connection details. The function will
#     import the module if present and attempt to read attributes `NEO4J_URI`,
#     `NEO4J_USER`, `NEO4J_PASSWORD` or similar names.
#     """
#     import importlib.util
#     import os

#     candidates = ["neo4j_config.py", os.path.join("nas", "neo4j_config.py")]
#     for c in candidates:
#         if os.path.exists(c):
#             spec = importlib.util.spec_from_file_location("repo_neo4j_config", c)
#             mod = importlib.util.module_from_spec(spec)
#             try:
#                 spec.loader.exec_module(mod)  # type: ignore
#             except Exception as e:
#                 logger.warning("Failed to import %s: %s", c, e)
#                 return None
#             cfg = {}
#             for name in ("NEO4J_URI", "URI", "URL"):
#                 if hasattr(mod, name):
#                     cfg["uri"] = getattr(mod, name)
#                     break
#             for name in ("NEO4J_USER", "USER"):
#                 if hasattr(mod, name):
#                     cfg["user"] = getattr(mod, name)
#                     break
#             for name in ("NEO4J_PASSWORD", "PASSWORD", "PASS"):
#                 if hasattr(mod, name):
#                     cfg["password"] = getattr(mod, name)
#                     break
#             return cfg
#     return None


def main() -> None:
    # parser = argparse.ArgumentParser(description="Safe Neo4j cleanup helpers (batched operations)")
    # parser.add_argument("--uri", required=False, help="Neo4j URI like bolt://localhost:7687")
    # parser.add_argument("--user", required=False, help="Neo4j user")
    # parser.add_argument("--password", required=False, help="Neo4j password")
    # parser.add_argument("--batch-size", type=int, default=1000, help="Batch size for deletions")

    # sub = parser.add_subparsers(dest="cmd")

    # sp = sub.add_parser("dry-count", help="Count nodes matching label/where")
    # sp.add_argument("--label", required=False)
    # sp.add_argument("--where", required=False)

    # sp = sub.add_parser("delete-label", help="Delete nodes by label (batched)")
    # sp.add_argument("--label", required=True)
    # sp.add_argument("--where", required=False)

    # sp = sub.add_parser("delete-orphans", help="Delete nodes with no relationships")

    # sp = sub.add_parser("remove-property", help="Remove a property from nodes of a label")
    # sp.add_argument("--label", required=True)
    # sp.add_argument("--prop", required=True)

    # sp = sub.add_parser("drop-constraint", help="Drop a constraint by name")
    # sp.add_argument("--name", required=True)

    # sp = sub.add_parser("drop-index", help="Drop an index by name")
    # sp.add_argument("--name", required=True)

    # args = parser.parse_args()

    # cfg = try_load_repo_config() or {}
    # uri = args.uri or cfg.get("uri") or "bolt://localhost:7687"
    # user = args.user or cfg.get("user") or "neo4j"
    # password = args.password or cfg.get("password") or "neo4j"

    # driver = get_driver(uri, user, password)

    try:
        session=driver.session()
        session.execute_write(drop_constraint, "arch_name")
        session.execute_write(drop_constraint, "layer_name")
        session.execute_write(drop_constraint, "dataset_name")
        session.execute_write(drop_constraint, "hardware_name")
        print("constraints dropped successfully")

        # session.execute_write(delete_nodes_by_label_batch, label="Architecture")
        
        # if args.cmd == "dry-count":
        #     cnt = count_nodes(driver, label=args.label, where_cypher=args.where)
        #     print(f"Count: {cnt}")

        # elif args.cmd == "delete-label":
        #     deleted = delete_nodes_by_label_batch(driver, args.label, where_cypher=args.where, batch_size=args.batch_size)
        #     print(f"Total deleted: {deleted}")

        # elif args.cmd == "delete-orphans":
        #     deleted = delete_orphan_nodes_batch(driver, batch_size=args.batch_size)
        #     print(f"Total orphan nodes deleted: {deleted}")

        # elif args.cmd == "remove-property":
        #     updated = remove_property_batch(driver, args.label, args.prop, batch_size=args.batch_size)
        #     print(f"Total nodes updated (property cleared): {updated}")

        # elif args.cmd == "drop-constraint":
        #     drop_constraint(driver, args.name)
        #     print("Constraint dropped (if existed)")

        # elif args.cmd == "drop-index":
        #     drop_index(driver, args.name)
        #     print("Index dropped (if existed)")

        # else:
        #     parser.print_help()
    finally:
        driver.close()


if __name__ == "__main__":
    main()
