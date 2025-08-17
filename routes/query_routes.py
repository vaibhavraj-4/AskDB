from flask import Blueprint, request, jsonify
from services.db_service import connect_to_db, get_schema, natural_to_sql, execute_query
from utils.ai_summary import generate_ai_response
import pandas as pd
import traceback
import json

query_bp = Blueprint("query_bp", __name__)
connections = {}


@query_bp.route("/connect", methods=["POST"])
def connect():
    data = request.json
    db_type = data.get("type")
    config = data.get("config")

    try:
        conn = connect_to_db(db_type, config)
        connections["active"] = {
            "db_type": db_type,
            "conn": conn,
            "config": config
        }
        return jsonify({"db_type": db_type, "status": "connected"})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@query_bp.route("/query", methods=["POST"])
def query():
    if "active" not in connections:
        return jsonify({"error": "No active database connection"}), 400

    active = connections["active"]
    db_type = active["db_type"]
    conn = active["conn"]
    config = active.get("config", {})

    data = request.json
    user_prompt = data.get("prompt", "").strip()
    if not user_prompt:
        return jsonify({"error": "Empty prompt"}), 400

    try:
        # 1) Introspect schema (pass config for non-SQL DBs)
        schema = get_schema(conn, db_type, config)

        # 2) Convert NL -> query (SQL string for SQL DBs, JSON plan for others)
        plan_or_sql = natural_to_sql(user_prompt, schema, db_type)

        # 3) Execute
        rows, columns = execute_query(conn, db_type, plan_or_sql, config)

        # 4) Summarize + (auto) chart
        ai = generate_ai_response(rows, columns, user_prompt)

        # 5) Table HTML
        df = pd.DataFrame(rows, columns=columns)
        table_html = df.to_html(classes="table table-striped", index=False) if not df.empty else ""

        response_data = {
            "sql": plan_or_sql if isinstance(plan_or_sql, str) else json.dumps(plan_or_sql, indent=2),
            "summary": ai.get("summary", ""),
            "table_html": table_html
        }
        if ai.get("chart"):
            response_data["chart"] = ai["chart"]

        return jsonify(response_data)

    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 400
