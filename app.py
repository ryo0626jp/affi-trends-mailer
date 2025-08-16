#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Trending products → affiliate URLs → Excel → Email (JST)
- HTML本文に表（楽天/Amazonリンク）
- さらに「はてなHTMLエディタにそのまま貼れる」CTAスニペットを
  .txt と .html の2種類で添付（iPhoneでコピーしやすい）
- 楽天APIは429/400対策、キーワード整形、ペース調整済み
"""
import os, re, json, time, logging, pathlib, datetime as dt, smtplib
from typing import List, Union, Optional
from urllib.parse import urlencode, quote_plus

import pandas as pd
from pytrends.request import TrendReq
import requests, pytz
from email.message import EmailMessage
from email.utils import formatdate, make_msgid
import html as htmlmod

APP_DIR = pathlib.Path(__file__).resolve().parent
OUTPUT_XLSX = APP_DIR / "trending_affiliates.xlsx"
LOG_FILE = APP_DIR / "run.log"

RAKUTEN_APP_ID = os.getenv("RAKUTEN_APPLICATION_ID", "")
RAKUTEN_AFFILIATE_ID = os.getenv("RAKUTEN_AFFILIATE_ID", "")
AMAZON_ASSOCIATE_TAG = os.getenv("AMAZON_ASSOCIATE_TAG", "")
NO_FILTER = os.getenv("NO_FILTER", "0") == "1"

EMAIL_CFG = {}
CONFIG_JSON = APP_DIR / "config.json"
if CONFIG_JSON.exists():
    try:
        cfg = json.loads(CONFIG_JSON.read_text(encoding="utf-8"))
        RAKUTEN_APP_ID = RAKUTEN_APP_ID or cfg.get("RAKUTEN_APPLICATION_ID", "")
        RAKUTEN_AFFILIATE_ID = RAKUTEN_AFFILIATE_ID or cfg.get("RAKUTEN_AFFILIATE_ID", "")
        AMAZON_ASSOCIATE_TAG = AMAZON_ASSOCIATE_TAG or cfg.get("AMAZON_ASSOCIATE_TAG", "")
        EMAIL_CFG = cfg.get("EMAIL", {}) or {}
    except Exception:
        pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("affi_mail")
JST = pytz.timezone("Asia/Tokyo")

def jst_now():
    return dt.datetime.now(JST)

def get_top_trends_japan(n=20):
    pytrends = TrendReq(hl='ja-JP', tz=540)
    try:
        df = pytrends.trending_searches(pn='japan')
        if df is not None and not df.empty:
            return df[0].astype(str).head(n).tolist()
    except Exception as e:
        logger.warning(f"trending_searches failed: {e}")
    try:
        df = pytrends.today_searches(pn='JP')
        if df is not None and not df.empty:
            try:
                return df.astype(str).head(n).tolist()
            except Exception:
                if 0 in df.columns:
                    return df[0].astype(str).head(n).tolist()
                if 'query' in df.columns:
                    return df['query'].astype(str).head(n).tolist()
    except Exception as e:
        logger.warning(f"today_searches failed: {e}")
    try:
        df = pytrends.realtime_trending_searches(pn='JP')
        if df is not None and not df.empty:
            if 'title' in df.columns:
                out = []
                for v in df['title'].tolist():
                    if isinstance(v, dict) and 'query' in v:
                        out.append(v['query'])
                    else:
                        out.append(str(v))
                return out[:n]
            if 'query' in df.columns:
                return df['query'].astype(str).head(n).tolist()
    except Exception as e:
        logger.error(f"realtime_trending_searches failed: {e}")
    return []

def fallback_keywords_from_rakuten_ranking(limit=20):
    if not RAKUTEN_APP_ID:
        logger.warning("Rakuten AppID missing; cannot fallback to Rakuten ranking.")
        return []
    endpoint = "https://app.rakuten.co.jp/services/api/IchibaItem/Ranking/20170628"
    params = {"applicationId": RAKUTEN_APP_ID, "format": "json", "genreId": 0, "page": 1}
    try:
        r = requests.get(endpoint, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        items = data.get("Items", [])
        names = []
        for it in items:
            name = it.get("Item", {}).get("itemName")
            if name:
                names.append(name)
            if len(names) >= limit:
                break
        return names
    except Exception as e:
        logger.error(f"Rakuten ranking fallback failed: {e}")
        return []

_MODEL_TOKEN = re.compile(r"[A-Za-z]*\d{2,}[A-Za-z0-9\-]*")
_KATAKANA = re.compile(r"[\u30A0-\u30FF]")
def is_productish(term: str) -> bool:
    t = term.strip()
    if len(t) <= 1: return False
    if _MODEL_TOKEN.search(t): return True
    if _KATAKANA.search(t): return True
    for hint in ["レビュー", "比較", "おすすめ", "型番", "最安値"]:
        if hint in t: return True
    if re.search(r"[A-Za-z]", t) and len(t) <= 20: return True
    return False

def sanitize_keyword(s: str) -> str:
    s = re.sub(r"[\u3000\s]+", " ", s)
    s = re.sub(r"[^\w\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FFF\-\+A-Za-z0-9 ]", " ", s)
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s[:120]

def rakuten_search_first_affiliate_url(keyword: str) -> str:
    if not RAKUTEN_APP_ID or not RAKUTEN_AFFILIATE_ID:
        logger.warning("Rakuten IDs not set; skipping Rakuten URL.")
        return ""
    endpoint = "https://app.rakuten.co.jp/services/api/IchibaItem/Search/20220601"
    kw = sanitize_keyword(keyword)
    for attempt in range(5):
        params = {"applicationId": RAKUTEN_APP_ID, "affiliateId": RAKUTEN_AFFILIATE_ID,
                  "format": "json", "keyword": kw, "hits": 1, "sort": "-reviewCount"}
        try:
            resp = requests.get(endpoint, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            items = data.get("Items", [])
            if not items:
                logger.info(f"Rakuten: no items for '{kw}'")
                return ""
            item = items[0].get("Item", {})
            return item.get("affiliateUrl") or item.get("itemUrl", "")
        except requests.HTTPError as e:
            status = getattr(e.response, "status_code", None)
            if status == 429:
                wait = 1.2 * (attempt + 1)
                logger.warning(f"Rakuten 429; retrying in {wait:.1f}s (attempt {attempt+1}/5)")
                time.sleep(wait); continue
            if status == 400 and len(kw) > 40:
                kw = " ".join(kw.split()[:6])
                logger.warning(f"Rakuten 400; shorten keyword and retry: '{kw}'")
                continue
            logger.error(f"Rakuten API HTTPError ({status}) for '{kw}': {e}")
            return ""
        except Exception as e:
            logger.error(f"Rakuten API error for '{kw}': {e}")
            return ""
    return ""

def amazon_search_url(keyword: str) -> str:
    base = "https://www.amazon.co.jp/s"
    q = {"k": keyword}
    if AMAZON_ASSOCIATE_TAG: q["tag"] = AMAZON_ASSOCIATE_TAG
    return f"{base}?{urlencode(q, quote_via=quote_plus)}"

def append_to_excel(rows):
    df_new = pd.DataFrame(rows, columns=["timestamp", "date", "keyword", "rakuten_url", "amazon_url"])
    if OUTPUT_XLSX.exists():
        try:
            df_old = pd.read_excel(OUTPUT_XLSX)
            df_all = pd.concat([df_old, df_new], ignore_index=True)
        except Exception as e:
            logger.warning(f"Failed to read existing Excel, recreating. Reason: {e}")
            df_all = df_new
    else:
        df_all = df_new
    if not df_all.empty:
        df_all["date"] = pd.to_datetime(df_all["date"]).dt.date
        df_all = df_all.drop_duplicates(subset=["date", "keyword"], keep="first")
    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl", mode="w") as writer:
        df_all.to_excel(writer, index=False)
    logger.info(f"Excel updated: {OUTPUT_XLSX} ({len(df_all)} rows total)")

# ===== Email bodies & attachments =====
def link_label(url: str) -> str:
    if not url: return "商品を見る"
    u = url.lower()
    if "rakuten.co.jp" in u: return "楽天で見る"
    if "amazon.co.jp" in u: return "Amazonで見る"
    return "商品を見る"

def build_copy_snippets(rows: List[dict]) -> str:
    """Return raw HTML CTA snippets (unescaped) for Hatena HTML editor."""
    parts = []
    for r in rows:
        kw = str(r.get("keyword","")).strip()
        url = r.get("rakuten_url") or r.get("amazon_url") or ""
        if not url: continue
        label = link_label(url)
        parts.append(f"<!-- {kw} -->\n"
                     f"<div style=\"margin:16px 0;\">\n"
                     f"  <a href=\"{url}\" rel=\"nofollow sponsored\" "
                     f"style=\"display:inline-block;padding:12px 16px;background:#2563eb;color:#fff;border-radius:8px;text-decoration:none;font-weight:700;\">{label}</a>\n"
                     f"</div>\n")
    return "\n".join(parts).strip()

def build_email_bodies(rows: List[dict], ts: dt.datetime) -> (str, str):
    """Return (plain_text, html) bodies; html includes table + notice about attachments."""
    timestamp = ts.strftime("%Y-%m-%d %H:%M JST")
    if not rows:
        plain = f"自動送信: トレンドが取得できませんでした。空のシートを添付します。\n生成時刻: {timestamp}\n"
        html = f"""<p>自動送信: トレンドが取得できませんでした。空のシートを添付します。<br>生成時刻: {htmlmod.escape(timestamp)}</p>"""
        return plain, html

    # Plain text
    lines = [f"自動送信: 本日のトレンド商品一覧（{timestamp}）", f"件数: {len(rows)}", ""]
    for r in rows:
        kw = r.get("keyword","")
        lines.append(f"- {kw}")
        if r.get("rakuten_url"):
            lines.append(f"   楽天: {r['rakuten_url']}")
        if r.get("amazon_url"):
            lines.append(f"   Amazon: {r['amazon_url']}")
    plain = "\n".join(lines)

    # HTML
    rows_html = []
    for r in rows:
        kw = htmlmod.escape(str(r.get("keyword","")))
        rurl = r.get("rakuten_url") or ""
        aurl = r.get("amazon_url") or ""
        rlink = f'<a href="{htmlmod.escape(rurl)}">楽天</a>' if rurl else ""
        alink = f'<a href="{htmlmod.escape(aurl)}">Amazon</a>' if aurl else ""
        rows_html.append(f"<tr><td>{kw}</td><td>{rlink}</td><td>{alink}</td></tr>")
    table = "\n".join(rows_html)
    note = ("<p style='color:#555;font-size:12px'>※ はてな<strong>HTMLエディタ</strong>に貼る用のCTAコードを "
            "<strong>hatena_cta_snippets.txt</strong> と <strong>.html</strong> で添付しています。"
            "iPhoneなら添付を開いて全選択→コピーで貼り付け可能です。</p>")
    html = f"""
    <div>
      <p>自動送信: 本日のトレンド商品一覧（{htmlmod.escape(timestamp)}）</p>
      <p>件数: {len(rows)}</p>
      <table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse">
        <thead><tr><th>キーワード</th><th>楽天</th><th>Amazon</th></tr></thead>
        <tbody>{table}</tbody>
      </table>
      {note}
    </div>
    """
    return plain, html

def send_email_with_attachments(
    smtp_host: str,
    smtp_port: int,
    smtp_user: str,
    smtp_password: str,
    mail_from: str,
    mail_to: Union[str, List[str]],
    subject: str,
    body_text: str,
    attachment_main_xlsx: pathlib.Path,
    body_html: Optional[str] = None,
    extra_attachments: Optional[List[pathlib.Path]] = None
) -> None:
    recipients = [mail_to] if isinstance(mail_to, str) else list(mail_to)

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = mail_from
    msg["To"] = ", ".join(recipients)
    msg["Date"] = formatdate(localtime=True)
    msg["Message-ID"] = make_msgid()

    # Plain + HTML
    msg.set_content(body_text)
    if body_html:
        msg.add_alternative(body_html, subtype="html")

    # Attach Excel
    with open(attachment_main_xlsx, "rb") as f:
        data = f.read()
    msg.add_attachment(data, maintype="application",
                       subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                       filename=attachment_main_xlsx.name)

    # Extra attachments
    for p in (extra_attachments or []):
        with open(p, "rb") as f:
            data = f.read()
        # Guess simple subtype
        if p.suffix.lower() == ".txt":
            msg.add_attachment(data, maintype="text", subtype="plain", filename=p.name)
        elif p.suffix.lower() in (".htm", ".html"):
            msg.add_attachment(data, maintype="text", subtype="html", filename=p.name)
        else:
            msg.add_attachment(data, maintype="application", subtype="octet-stream", filename=p.name)

    # Send
    with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
        server.ehlo()
        if int(smtp_port) in (587, 25):
            server.starttls(); server.ehlo()
        if smtp_user:
            server.login(smtp_user, smtp_password)
        server.send_message(msg)

    logger.info("Email sent to %s with %s and %d extra attachments",
                recipients, attachment_main_xlsx.name, len(extra_attachments or []))

# ===== Main =====
def main(top_n=20):
    ts = jst_now()
    trends = get_top_trends_japan(n=top_n)
    logger.info("Trends(raw)=%s", trends)

    if not trends:
        logger.warning("No Google trends; falling back to Rakuten ranking.")
        trends = fallback_keywords_from_rakuten_ranking(limit=top_n)
        logger.info("Fallback trends=%s", trends)
        if not trends:
            append_to_excel([])
            if email_enabled():
                subj = (EMAIL_CFG.get("SUBJECT", "トレンド商品レポート（{date} {time}）")
                        .format(date=ts.strftime("%Y-%m-%d"), time=ts.strftime("%H:%M")))
                plain, html = build_email_bodies([], ts)
                send_email_with_attachments(
                    EMAIL_CFG["SMTP_HOST"], int(EMAIL_CFG["SMTP_PORT"]),
                    EMAIL_CFG["SMTP_USER"], EMAIL_CFG["SMTP_PASSWORD"],
                    EMAIL_CFG["FROM"], EMAIL_CFG["TO"], subj, plain, OUTPUT_XLSX, body_html=html
                )
            return 1

    product_terms = trends if NO_FILTER else [t for t in trends if is_productish(t)]
    logger.info("Product-like=%s", product_terms)

    rows = []
    for term in product_terms:
        r_url = rakuten_search_first_affiliate_url(term)
        a_url = amazon_search_url(term)
        rows.append({
            "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S%z"),
            "date": ts.strftime("%Y-%m-%d"),
            "keyword": term,
            "rakuten_url": r_url,
            "amazon_url": a_url
        })
        time.sleep(0.8)

    append_to_excel(rows)

    # Build copy-ready snippets and write files
    extra_paths = []
    snippets = build_copy_snippets(rows)
    if snippets:
        txt_path = APP_DIR / "hatena_cta_snippets.txt"
        html_path = APP_DIR / "hatena_cta_snippets.html"
        txt_path.write_text(snippets, encoding="utf-8")
        # Simple HTML wrapper so iPhoneで開いてコピーしやすい
        html_wrapper = f"<!doctype html><meta charset='utf-8'><pre>{htmlmod.escape(snippets)}</pre>"
        html_path.write_text(html_wrapper, encoding="utf-8")
        extra_paths.extend([txt_path, html_path])

    if email_enabled():
        count = len(rows)
        subj = (EMAIL_CFG.get("SUBJECT", "トレンド商品レポート（{date} {time}）")
                .format(date=ts.strftime("%Y-%m-%d"), time=ts.strftime("%H:%M")))
        plain, html = build_email_bodies(rows, ts)
        send_email_with_attachments(
            EMAIL_CFG["SMTP_HOST"], int(EMAIL_CFG["SMTP_PORT"]),
            EMAIL_CFG["SMTP_USER"], EMAIL_CFG["SMTP_PASSWORD"],
            EMAIL_CFG["FROM"], EMAIL_CFG["TO"], subj, plain, OUTPUT_XLSX,
            body_html=html, extra_attachments=extra_paths
        )
    else:
        logger.warning("Email not sent: EMAIL config incomplete.")

    logger.info("Done.")
    return 0

def email_enabled():
    req_keys = ["SMTP_HOST", "SMTP_PORT", "SMTP_USER", "SMTP_PASSWORD", "FROM", "TO"]
    for k in req_keys:
        if not EMAIL_CFG.get(k):
            logger.warning("EMAIL config missing key: %s", k)
            return False
    return True

if __name__ == "__main__":
    exit(main(20))
