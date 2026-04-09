#!/usr/bin/env python3
"""
Notion → data.json sync for 식물행성 대시보드.

Reads pages from Notion DB '📱 인스타그램 콘텐츠 파이프라인' and regenerates
data.json keyed by '식물/주제 키' (canonical card_id).

ENV:
    NOTION_TOKEN — Notion integration token with read access to the DB
"""
import json
import os
import sys
import urllib.request
import urllib.error
from datetime import datetime, timezone

DATABASE_ID = "7cf5c9cd-b05a-4827-a83f-ff3a106a3b88"
NOTION_VERSION = "2022-06-28"
OUT_PATH = "data.json"
SLIDES_PATH = "slides.json"

SECTION_MAP = {
    "📤 발행완료": "published",
    "✅ 승인대기": "scheduled",
    "📤 업로드하기": "scheduled",
    "🎨 제작중": "queued",
    "🔍 주제선정됨": "queued",
    "⏭️ 스킵": "queued",
}


def notion_query(token, database_id):
    """Query all pages from a Notion database (handles pagination)."""
    pages = []
    start_cursor = None
    while True:
        body = {"page_size": 100}
        if start_cursor:
            body["start_cursor"] = start_cursor
        req = urllib.request.Request(
            f"https://api.notion.com/v1/databases/{database_id}/query",
            data=json.dumps(body).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {token}",
                "Notion-Version": NOTION_VERSION,
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read())
        except urllib.error.HTTPError as e:
            sys.stderr.write(f"Notion API error {e.code}: {e.read().decode()}\n")
            raise
        pages.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        start_cursor = data.get("next_cursor")
    return pages


def get_title(prop):
    if not prop or prop.get("type") != "title":
        return ""
    return "".join(t.get("plain_text", "") for t in prop.get("title", []))


def get_rich_text(prop):
    if not prop or prop.get("type") != "rich_text":
        return ""
    return "".join(t.get("plain_text", "") for t in prop.get("rich_text", []))


def get_select(prop):
    if not prop or prop.get("type") != "select":
        return None
    sel = prop.get("select")
    return sel.get("name") if sel else None


def get_date(prop):
    if not prop or prop.get("type") != "date":
        return None
    d = prop.get("date")
    return d.get("start") if d else None


def get_url(prop):
    if not prop or prop.get("type") != "url":
        return None
    return prop.get("url")


def get_number(prop):
    if not prop or prop.get("type") != "number":
        return None
    return prop.get("number")


# Map Notion '콘텐츠 타입' select names → dashboard card type
TYPE_MAP = {
    "랭킹": "ranking",
    "문제해결": "problem",
    "대화형": "conversation",
    "트러블슈팅": "problem",
    "ranking": "ranking",
    "problem": "problem",
    "conversation": "conversation",
}


def normalize_type(raw):
    if not raw:
        return "conversation"
    return TYPE_MAP.get(raw, raw if raw in ("ranking", "problem", "conversation") else "conversation")


def page_to_card(page, slides_db):
    props = page.get("properties", {})
    key = get_rich_text(props.get("식물/주제 키"))
    title = get_title(props.get("콘텐츠명"))
    status = get_select(props.get("상태"))
    ctype_raw = get_select(props.get("콘텐츠 타입"))
    scheduled = get_date(props.get("예정 발행일"))
    published = get_date(props.get("실제 발행일"))
    ig_url = get_url(props.get("인스타그램 URL"))
    likes = get_number(props.get("좋아요"))
    comments = get_number(props.get("댓글"))

    if not key:
        key = "notion_" + page.get("id", "").replace("-", "")[:12]

    section = SECTION_MAP.get(status or "", "queued")
    slides_entry = slides_db.get(key) or {}
    slides_urls = slides_entry.get("slides") or []
    cover = slides_urls[0] if slides_urls else ""
    # Prefer slides.json type (canonical render type); fall back to Notion
    render_type = slides_entry.get("type") or ctype_raw
    ctype = normalize_type(render_type)

    # Dashboard date: scheduled uses 예정, published/queued fall back appropriately
    if section == "published":
        date_val = published or scheduled or ""
    elif section == "scheduled":
        date_val = scheduled or published or ""
    else:
        date_val = scheduled or published or ""

    n_slides = len(slides_urls)
    n_total = max(n_slides, 8 if ctype != "problem" else 8)  # default 8-slide carousel

    return {
        "id": key,
        "title": title or slides_entry.get("title") or "(제목 없음)",
        "type": ctype,
        "date": date_val or "",
        "cover": cover,
        "slides": slides_urls,
        "n_slides": n_slides,
        "n_total": n_total,
        "status": status or "— 미설정",
        "ig_url": ig_url or "",
        "section": section,
        "notion_page_id": page.get("id", ""),
        "notion_url": page.get("url"),
        "likes": likes,
        "comments": comments,
    }


def main():
    token = os.environ.get("NOTION_TOKEN")
    if not token:
        sys.stderr.write("NOTION_TOKEN env var is required\n")
        sys.exit(2)

    print(f"[sync] querying Notion DB {DATABASE_ID}")
    pages = notion_query(token, DATABASE_ID)
    print(f"[sync] got {len(pages)} pages")

    # Load slides.json so each card gets cover/slides/n_slides/n_total
    slides_db = {}
    if os.path.exists(SLIDES_PATH):
        try:
            with open(SLIDES_PATH, encoding="utf-8") as f:
                raw = json.load(f)
            slides_db = {k: v for k, v in raw.items() if k != "_comment"}
        except Exception as e:
            sys.stderr.write(f"[sync] WARN: could not read {SLIDES_PATH}: {e}\n")

    cards = [page_to_card(p, slides_db) for p in pages]
    # Exclude cards without an explicit status (dashboard should only show triaged items)
    cards = [c for c in cards if c.get("status") and c["status"] != "— 미설정"]

    # Sort: scheduled asc by date, published desc, queued by title
    section_order = {"scheduled": 0, "queued": 1, "published": 2}
    def sort_key(c):
        sec = section_order.get(c["section"], 3)
        if c["section"] == "scheduled":
            return (sec, c.get("date") or "9999-12-31")
        if c["section"] == "published":
            return (sec, "-" + (c.get("date") or "0000-01-01"))
        return (sec, c.get("title") or "")
    cards.sort(key=sort_key)

    counts = {"scheduled": 0, "queued": 0, "published": 0}
    for c in cards:
        counts[c["section"]] = counts.get(c["section"], 0) + 1

    out = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "notion-sync",
        "database_id": DATABASE_ID,
        "counts": counts,
        "cards": cards,
    }
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"[sync] wrote {OUT_PATH}: {len(cards)} cards ({counts})")


if __name__ == "__main__":
    main()
