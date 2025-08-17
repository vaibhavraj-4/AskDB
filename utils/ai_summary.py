import os
import io
import base64
import json
from datetime import datetime

import google.generativeai as genai
from dotenv import load_dotenv

# Charting
import matplotlib
matplotlib.use("Agg")  # headless
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

load_dotenv()
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))


def _pick_chart_spec(df: pd.DataFrame):
    """
    Heuristics to decide a reasonable chart:
    - If there's a datetime-like column + one numeric -> line chart over time.
    - If there's exactly 1 categorical-like column + 1 numeric -> bar chart (top 20).
    - Else: no chart.
    Returns: dict {type: 'line'|'bar'|None, x: str, y: str}
    """
    if df.empty or df.shape[1] < 2:
        return {"type": None}

    # classify columns
    numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    # try to coerce datetimes
    datetime_cols = []
    for c in df.columns:
        if df[c].dtype == "datetime64[ns]":
            datetime_cols.append(c)
        else:
            # try parse
            try:
                parsed = pd.to_datetime(df[c], errors="raise", infer_datetime_format=True)
                # only accept if at least 60% parsed uniquely
                if parsed.notna().mean() > 0.6:
                    df[c] = parsed
                    datetime_cols.append(c)
            except Exception:
                pass

    categorical_cols = [c for c in df.columns if df[c].dtype == "object" or pd.api.types.is_categorical_dtype(df[c])]

    # time series
    if datetime_cols and numeric_cols:
        x = datetime_cols[0]
        y = numeric_cols[0]
        # aggregate by date if duplicates
        tmp = df[[x, y]].dropna()
        spec = {"type": "line", "x": x, "y": y}
        return spec

    # categorical bar: pick a label-like column and a numeric
    if categorical_cols and numeric_cols:
        x = categorical_cols[0]
        y = numeric_cols[0]
        return {"type": "bar", "x": x, "y": y}

    return {"type": None}


def _plot_to_base64(fig):
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")


def _make_chart(df: pd.DataFrame):
    spec = _pick_chart_spec(df.copy())
    if not spec or spec["type"] is None:
        return None

    kind = spec["type"]
    x, y = spec["x"], spec["y"]

    # Reduce to top 20 for readability on bar charts
    plot_df = df[[x, y]].dropna()
    if kind == "bar":
        # try "top by y"
        # if duplicates for labels, aggregate
        plot_df = plot_df.groupby(x, as_index=False)[y].sum()
        plot_df = plot_df.sort_values(y, ascending=False).head(20)

        fig, ax = plt.subplots(figsize=(8, 4.5))
        ax.bar(plot_df[x].astype(str), plot_df[y])
        ax.set_xlabel(x)
        ax.set_ylabel(y)
        ax.set_title(f"Top {min(20, len(plot_df))} by {y}")
        ax.tick_params(axis='x', labelrotation=45)

        return _plot_to_base64(fig)

    if kind == "line":
        # aggregate by date
        plot_df = plot_df.groupby(x, as_index=False)[y].sum().sort_values(x)

        fig, ax = plt.subplots(figsize=(8, 4.5))
        ax.plot(plot_df[x], plot_df[y])
        ax.set_xlabel(x)
        ax.set_ylabel(y)
        ax.set_title(f"{y} over {x}")

        return _plot_to_base64(fig)

    return None


def _gemini_summary(rows, columns, user_prompt):
    """
    Ask Gemini for a concise, business-friendly summary with the user's intent in mind.
    """
    model = genai.GenerativeModel("gemini-1.5-flash")

    # small preview (avoid pushing huge tables)
    preview_rows = rows[:30]
    preview = [dict(zip(columns, r)) for r in preview_rows] if columns else []

    prompt = f"""
You are a data analyst assistant. The user asked:
"{user_prompt}"

You have the following tabular data (sample, up to 30 rows):
{json.dumps(preview, default=str)[:8000]}

Write a concise business-style summary of the key insights (3-6 sentences).
If the result set is empty, explain likely reasons and suggest a next query.
Avoid repeating the raw numbers excessively—focus on the story.
"""
    resp = model.generate_content(prompt)
    return (resp.text or "").strip()


def generate_ai_response(rows, columns, user_prompt):
    """
    Returns:
    {
      "summary": str,
      "chart": <base64 png> | None
    }
    """
    # Build DataFrame (best-effort)
    df = pd.DataFrame(rows, columns=columns or [])

    # 1) Summary from Gemini
    summary = _gemini_summary(rows, columns, user_prompt)

    # 2) Auto chart (matplotlib, base64)
    chart_b64 = None
    try:
        if not df.empty and df.shape[1] >= 2:
            chart_b64 = _make_chart(df)
    except Exception:
        chart_b64 = None

    return {
        "summary": summary,
        "chart": chart_b64
    }
