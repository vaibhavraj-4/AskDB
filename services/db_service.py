import mysql.connector
import psycopg2
import pymongo
import redis
import json
import re
import os
from elasticsearch import Elasticsearch
import google.generativeai as genai
from dotenv import load_dotenv

# Firestore (Admin)
import firebase_admin
from firebase_admin import credentials, firestore

load_dotenv()
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))


# ---------- Connections ----------

def connect_to_db(db_type, config):
    """
    Connect to various database types with appropriate configuration.
    - For Firebase, pass the *service account JSON object* in config["config"].
    """
    try:
        if db_type == "mysql":
            return mysql.connector.connect(
                host=config.get("host"),
                port=int(config.get("port", 3306)),
                database=config.get("database"),
                user=config.get("user"),
                password=config.get("password")
            )

        elif db_type == "postgres":
            return psycopg2.connect(
                host=config.get("host"),
                port=int(config.get("port", 5432)),
                database=config.get("database"),
                user=config.get("user"),
                password=config.get("password")
            )

        elif db_type == "mongodb":
            return pymongo.MongoClient(
                host=config.get("host"),
                port=int(config.get("port", 27017)),
                username=config.get("user") or None,
                password=config.get("password") or None,
                authSource=config.get("database") or "admin"
            )

        elif db_type == "redis":
            return redis.Redis(
                host=config.get("host"),
                port=int(config.get("port", 6379)),
                password=config.get("password") or None,
                decode_responses=True
            )

        elif db_type == "firebase":
            # Expect service account JSON (dict) in config["config"]
            sa = config.get("config")
            if isinstance(sa, str):
                try:
                    sa = json.loads(sa)
                except Exception:
                    pass  # might be a file path
            if not firebase_admin._apps:
                if isinstance(sa, dict):
                    cred = credentials.Certificate(sa)
                else:
                    cred = credentials.Certificate(sa)  # path string
                firebase_admin.initialize_app(cred)
            return firestore.client()

        elif db_type == "elasticsearch":
            return Elasticsearch([{
                "host": config.get("host"),
                "port": int(config.get("port", 9200)),
                "scheme": "http"
            }])

        elif db_type == "custom":
            raise NotImplementedError("Custom DB not implemented yet")

        else:
            raise ValueError(f"Unsupported database type: {db_type}")

    except Exception as e:
        raise ConnectionError(f"Failed to connect to {db_type}: {str(e)}")


# ---------- Schema Introspection ----------

def get_schema(conn, db_type, config=None):
    """
    Return a lightweight schema snapshot to steer the NL->Query step.
    """
    schema_info = []
    try:
        if db_type == "mysql":
            cur = conn.cursor()
            cur.execute("SHOW TABLES;")
            tables = [r[0] for r in cur.fetchall()]
            for t in tables:
                cur.execute(f"DESCRIBE `{t}`;")
                cols = [c[0] for c in cur.fetchall()]
                schema_info.append({"table": t, "columns": cols})
            cur.close()

        elif db_type == "postgres":
            cur = conn.cursor()
            cur.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema='public';
            """)
            tables = [r[0] for r in cur.fetchall()]
            for t in tables:
                cur.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema='public' AND table_name=%s;
                """, (t,))
                cols = [r[0] for r in cur.fetchall()]
                schema_info.append({"table": t, "columns": cols})
            cur.close()

        elif db_type == "mongodb":
            db = conn[config.get("database")]
            for coll in db.list_collection_names():
                doc = db[coll].find_one()
                fields = list(doc.keys()) if doc else []
                schema_info.append({"collection": coll, "fields": fields})

        elif db_type == "firebase":
            # Firestore is schemaless; list top-level collections and sample fields.
            cols = conn.collections()
            for c in cols:
                doc = next(c.limit(1).get(), None)
                fields = list(doc.to_dict().keys()) if doc else []
                schema_info.append({"collection": c.id, "fields": fields})

        elif db_type == "elasticsearch":
            index = (config or {}).get("index")
            if index:
                mapping = conn.indices.get_mapping(index=index)
                props = mapping[index]["mappings"].get("properties", {})
                schema_info.append({"index": index, "fields": list(props.keys())})

        elif db_type == "redis":
            schema_info.append({"note": "Key-value store; schema varies by key type"})

    except Exception as e:
        raise ValueError(f"Error getting schema: {str(e)}")

    return schema_info


# ---------- NL -> Query / Plan ----------

def _strip_code_fences(s: str) -> str:
    s = re.sub(r"```(?:\w+)?", "", s)
    return s.strip("` \n\r\t")


def natural_to_sql(prompt, schema, db_type):
    """
    For SQL DBs: returns a SQL string.
    For NoSQL: returns a JSON 'plan' the executor can run safely.

    MongoDB plan shape:
    {
      "collection": "students",
      "operation": "find" | "aggregate",
      "filter": {...},
      "projection": {...} | null,
      "sort": [["marks", -1]],
      "limit": 5,
      "pipeline": [...]         # if operation == "aggregate"
    }

    Firestore plan:
    {
      "collection": "students",
      "filters": [["marks", ">", 90]],
      "order_by": ["marks", "desc"],
      "limit": 5,
      "select": ["name","marks"] | null
    }

    Redis plan:
    { "command": "GET"|"SET"|"SCAN"|"HGETALL"|..., "args": [...] }

    Elasticsearch: return valid DSL JSON (dict).
    """
    model = genai.GenerativeModel("gemini-1.5-flash")
    if db_type in ["mysql", "postgres"]:
        sys = f"""
You are a strict {db_type.upper()} SQL generator.
Given the schema and the user request, return ONLY a single {db_type.upper()} query.
No commentary. No markdown. Avoid dangerous statements; prefer SELECT.
Schema:
{json.dumps(schema, indent=2)}
User request: "{prompt}"
"""
        resp = model.generate_content(sys)
        query = _strip_code_fences(resp.text or "")
        # Extract basic SQL if any extra text sneaks in
        m = re.search(r"(SELECT|INSERT|UPDATE|DELETE)\b.*", query, re.I | re.S)
        return m.group(0).strip() if m else query

    elif db_type == "mongodb":
        sys = f"""
You are a MongoDB query planner.
Return a SINGLE JSON object describing how to run the query safely (no code strings).
Use this schema snapshot and the user request.

Schema:
{json.dumps(schema, indent=2)}

User request: "{prompt}"

Rules:
- If a simple query, use operation "find" with "filter", optional "projection", "sort", "limit".
- If aggregation is clearly required, use operation "aggregate" and provide "pipeline".
- Do NOT include any explanation text. Return ONLY valid JSON.
"""
        resp = model.generate_content(sys)
        return json.loads(_strip_code_fences(resp.text or "{}"))

    elif db_type == "firebase":
        sys = f"""
You are a Firestore query planner.
Return a SINGLE JSON object with:
{{
  "collection": "<name>",
  "filters": [["field","op","value"], ...],  // op in: ==, >, >=, <, <=, in, array-contains
  "order_by": ["field","asc|desc"] | null,
  "limit": <int> | null,
  "select": ["field1","field2"] | null
}}
Use the schema snapshot and user request below. ONLY return JSON.

Schema:
{json.dumps(schema, indent=2)}

User request: "{prompt}"
"""
        resp = model.generate_content(sys)
        return json.loads(_strip_code_fences(resp.text or "{}"))

    elif db_type == "elasticsearch":
        sys = f"""
You are an Elasticsearch DSL generator.
Return ONLY a valid JSON body for the `search` API (no index).
Use the schema snapshot and user request.

Schema:
{json.dumps(schema, indent=2)}

User request: "{prompt}"
"""
        resp = model.generate_content(sys)
        return json.loads(_strip_code_fences(resp.text or "{}"))

    elif db_type == "redis":
        sys = f"""
You are a Redis command planner.
Return ONLY a JSON object: {{ "command": "<CMD>", "args": [ ... ] }}
Use simple commands like GET, SET, HGETALL, SCAN, ZREVRANGE, etc based on the user's request.
User request: "{prompt}"
"""
        resp = model.generate_content(sys)
        return json.loads(_strip_code_fences(resp.text or "{}"))

    else:
        raise ValueError(f"Unsupported database type for NL translation: {db_type}")


# ---------- Execute ----------

def execute_query(conn, db_type, plan_or_sql, config=None):
    """
    Execute SQL or execute a JSON plan for non-SQL DBs.
    Return rows (list[list]) and columns (list[str]).
    """
    try:
        if db_type in ["mysql", "postgres"]:
            sql = plan_or_sql if isinstance(plan_or_sql, str) else json.dumps(plan_or_sql)
            cur = conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            columns = [d[0] for d in cur.description] if cur.description else []
            cur.close()
            return rows, columns

        elif db_type == "mongodb":
            plan = plan_or_sql if isinstance(plan_or_sql, dict) else json.loads(plan_or_sql)
            db = conn[config.get("database")]
            coll = db[plan["collection"]]
            op = plan.get("operation", "find")

            if op == "find":
                cur = coll.find(plan.get("filter", {}), plan.get("projection") or None)
                if plan.get("sort"):
                    cur = cur.sort([(f, int(dir)) for f, dir in plan["sort"]])
                if plan.get("limit"):
                    cur = cur.limit(int(plan["limit"]))
                docs = list(cur)
                if not docs:
                    return [], []
                # flatten _id
                for d in docs:
                    d["_id"] = str(d.get("_id"))
                columns = sorted({k for d in docs for k in d.keys()})
                rows = [[d.get(c) for c in columns] for d in docs]
                return rows, columns

            elif op == "aggregate":
                pipeline = plan.get("pipeline", [])
                docs = list(coll.aggregate(pipeline))
                if not docs:
                    return [], []
                for d in docs:
                    if "_id" in d and not isinstance(d["_id"], (str, int, float)):
                        d["_id"] = str(d["_id"])
                columns = sorted({k for d in docs for k in d.keys()})
                rows = [[d.get(c) for c in columns] for d in docs]
                return rows, columns

            else:
                raise ValueError(f"Unsupported MongoDB operation: {op}")

        elif db_type == "firebase":
            plan = plan_or_sql if isinstance(plan_or_sql, dict) else json.loads(plan_or_sql)
            col = plan["collection"]
            q = conn.collection(col)
            for f in plan.get("filters", []) or []:
                field, op, value = f
                q = q.where(field, op, value)
            if plan.get("order_by"):
                f, direction = plan["order_by"]
                q = q.order_by(f, direction=direction)
            if plan.get("limit"):
                q = q.limit(int(plan["limit"]))
            docs = [d for d in q.stream()]
            if not docs:
                return [], []
            rows_dicts = [{**d.to_dict(), "id": d.id} for d in docs]
            columns = sorted({k for d in rows_dicts for k in d.keys()})
            rows = [[r.get(c) for c in columns] for r in rows_dicts]
            return rows, columns

        elif db_type == "elasticsearch":
            body = plan_or_sql if isinstance(plan_or_sql, dict) else json.loads(plan_or_sql)
            index = (config or {}).get("index")
            if not index:
                raise ValueError("Elasticsearch index is required")
            res = conn.search(index=index, body=body)
            hits = res.get("hits", {}).get("hits", [])
            if not hits:
                return [], []
            cols = sorted({k for h in hits for k in (h.get("_source") or {}).keys()})
            rows = [[(h.get("_source") or {}).get(c) for c in cols] for h in hits]
            return rows, cols

        elif db_type == "redis":
            plan = plan_or_sql if isinstance(plan_or_sql, dict) else json.loads(plan_or_sql)
            cmd = (plan.get("command") or "").upper()
            args = plan.get("args", [])
            # Very small router for common commands
            if cmd == "GET":
                val = conn.get(*args)
                return ([[val]] if val is not None else []), ["value"]
            elif cmd == "SET":
                ok = conn.set(*args)
                return [[bool(ok)]], ["ok"]
            elif cmd == "HGETALL":
                m = conn.hgetall(*args)
                if not m:
                    return [], []
                columns = ["field", "value"]
                rows = [[k, v] for k, v in m.items()]
                return rows, columns
            elif cmd == "SCAN":
                cursor, keys = conn.scan(*(int(a) if str(a).isdigit() else a for a in args))
                rows = [[k] for k in keys]
                return rows, ["key"]
            else:
                # Fallback: try to execute dynamically
                fn = getattr(conn, cmd.lower(), None)
                if not fn:
                    raise ValueError(f"Unsupported Redis command: {cmd}")
                res = fn(*args)
                if isinstance(res, (list, tuple)):
                    rows = [[x] if not isinstance(x, (list, tuple)) else list(x) for x in res]
                    columns = ["value"] if rows and len(rows[0]) == 1 else [f"col{i+1}" for i in range(len(rows[0]))]
                    return rows, columns
                return [[res]], ["result"]

        else:
            raise ValueError(f"Query execution not implemented for {db_type}")

    except Exception as e:
        raise ValueError(f"Error executing query: {str(e)}")
