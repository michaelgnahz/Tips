import json
from collections import Counter, defaultdict
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse


ROOT = Path("20260407-09")
TOP = 30


def params_from_url(value):
    if not value:
        return {}
    parsed = urlparse(value)
    return {key: values[0] for key, values in parse_qs(parsed.query).items() if values}


def user_key(item):
    return (
        item.get("userId")
        or item.get("distinct_id")
        or item.get("$device_id")
        or item.get("oaid")
        or item.get("uuid")
        or "unknown"
    )


def clean(value):
    return str(value or "(empty)").replace("|", "/")


def pdf_name(target):
    path = urlparse(target).path
    name = unquote(path.rsplit("/", 1)[-1]) if path else target
    return name or target


def pct(value, base):
    return f"{value / base:.2%}" if base else "0.00%"


scene_sessions = defaultdict(set)
scene_chatresp_sessions = defaultdict(set)
scene_suggestion_sessions = defaultdict(set)
scene_btn_sessions = defaultdict(set)
scene_pdf_sessions = defaultdict(set)

button_clicks = 0
pdf_clicks = 0
pdf_users = set()
pdf_sessions = set()

pdf_by_source = Counter()
pdf_by_name = Counter()
pdf_by_target = Counter()
pdf_pair = Counter()
pdf_by_btn = Counter()
pdf_by_day = Counter()
pdf_by_hour = Counter()
pdf_by_app_version = Counter()
pdf_by_html_version = Counter()
pdf_by_brand = Counter()
pdf_user_clicks = Counter()
pdf_session_clicks = Counter()

for path in sorted(ROOT.rglob("*.log")):
    with path.open(encoding="utf-8", errors="replace") as handle:
        for line in handle:
            item = json.loads(line)
            event = item.get("event")
            session = item.get("$session_id") or "unknown"
            user = user_key(item)
            source_params = params_from_url(item.get("$current_url") or item.get("$session_entry_url"))
            source_scene = item.get("scene") or source_params.get("scene") or "(empty)"

            scene_sessions[source_scene].add(session)
            if event == "chatResp":
                scene_chatresp_sessions[source_scene].add(session)
            elif event == "suggestion":
                scene_suggestion_sessions[source_scene].add(session)
            elif event == "btnClick":
                button_clicks += 1
                scene_btn_sessions[source_scene].add(session)
                target = item.get("target") or ""
                if ".pdf" not in target.lower():
                    continue

                name = pdf_name(target)
                timestamp = item.get("$time")
                hour = "unknown"
                day = "unknown"
                if isinstance(timestamp, (int, float)):
                    from datetime import datetime, timezone

                    hour = datetime.fromtimestamp(timestamp, tz=timezone.utc).astimezone().strftime("%Y-%m-%d %H:00")
                    day = hour[:10]

                pdf_clicks += 1
                pdf_users.add(user)
                pdf_sessions.add(session)
                scene_pdf_sessions[source_scene].add(session)
                pdf_by_source[source_scene] += 1
                pdf_by_name[name] += 1
                pdf_by_target[target] += 1
                pdf_pair[(source_scene, name)] += 1
                pdf_by_btn[item.get("btnName") or "(empty)"] += 1
                pdf_by_day[day] += 1
                pdf_by_hour[hour] += 1
                pdf_by_app_version[item.get("version") or source_params.get("app_version") or "(empty)"] += 1
                pdf_by_html_version[item.get("htmlVersion") or "(empty)"] += 1
                pdf_by_brand[source_params.get("detected_brand") or "(empty)"] += 1
                pdf_user_clicks[user] += 1
                pdf_session_clicks[session] += 1


print("# PDF 下载分析")
print()
print("## 总览")
print(f"- 按钮点击总数: {button_clicks:,}")
print(f"- PDF 点击数: {pdf_clicks:,}")
print(f"- PDF 点击占按钮点击: {pct(pdf_clicks, button_clicks)}")
print(f"- PDF 点击用户数: {len(pdf_users):,}")
print(f"- PDF 点击会话数: {len(pdf_sessions):,}")
print(f"- 人均 PDF 点击: {pdf_clicks / len(pdf_users):.2f}" if pdf_users else "- 人均 PDF 点击: 0")
print(f"- 会话均 PDF 点击: {pdf_clicks / len(pdf_sessions):.2f}" if pdf_sessions else "- 会话均 PDF 点击: 0")
print()

print("## PDF 来源场景")
print("| 来源场景 | PDF点击 | 场景会话数 | PDF点击会话率 | chatResp会话率 | suggestion会话率 |")
print("| --- | ---: | ---: | ---: | ---: | ---: |")
for scene, count in pdf_by_source.most_common(TOP):
    base = len(scene_sessions[scene])
    print(
        f"| {clean(scene)} | {count:,} | {base:,} | {pct(len(scene_pdf_sessions[scene]), base)} | "
        f"{pct(len(scene_chatresp_sessions[scene]), base)} | {pct(len(scene_suggestion_sessions[scene]), base)} |"
    )
print()

print("## PDF 文件")
print("| PDF | 点击数 | 占PDF点击 |")
print("| --- | ---: | ---: |")
for name, count in pdf_by_name.most_common(TOP):
    print(f"| {clean(name)} | {count:,} | {pct(count, pdf_clicks)} |")
print()

print("## 来源场景到 PDF")
print("| 来源场景 | PDF | 点击数 |")
print("| --- | --- | ---: |")
for (scene, name), count in pdf_pair.most_common(TOP):
    print(f"| {clean(scene)} | {clean(name)} | {count:,} |")
print()

print("## PDF 完整目标")
print("| target | 点击数 |")
print("| --- | ---: |")
for target, count in pdf_by_target.most_common(TOP):
    print(f"| {clean(target)} | {count:,} |")
print()

for title, counter in [
    ("按钮名", pdf_by_btn),
    ("每日趋势", pdf_by_day),
    ("App 版本", pdf_by_app_version),
    ("HTML 版本", pdf_by_html_version),
    ("品牌", pdf_by_brand),
]:
    print(f"## {title}")
    print("| 值 | 点击数 | 占比 |")
    print("| --- | ---: | ---: |")
    for key, count in counter.most_common(TOP):
        print(f"| {clean(key)} | {count:,} | {pct(count, pdf_clicks)} |")
    print()

print("## 小时趋势")
print("| 小时 | PDF点击 |")
print("| --- | ---: |")
for hour, count in sorted(pdf_by_hour.items()):
    print(f"| {clean(hour)} | {count:,} |")
print()

print("## 重复点击")
print(f"- PDF 点击超过 1 次的用户数: {sum(1 for value in pdf_user_clicks.values() if value > 1):,}")
print(f"- PDF 点击超过 1 次的会话数: {sum(1 for value in pdf_session_clicks.values() if value > 1):,}")
