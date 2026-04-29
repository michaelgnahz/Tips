import json
from collections import Counter, defaultdict
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse


ROOT = Path("20260407-09")
TOP = 20


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


def text(value):
    value = value or "(empty)"
    return str(value).replace("|", "/")


def pct(value, base):
    return f"{value / base:.2%}" if base else "0.00%"


scene_sessions = defaultdict(set)
scene_suggestion_sessions = defaultdict(set)
scene_chatresp_sessions = defaultdict(set)
scene_btn_sessions = defaultdict(set)

button_total = 0
related_clicks = 0
app_clicks = 0
hotline_clicks = 0
other_clicks = 0

related_users = set()
related_sessions = set()
app_users = set()
app_sessions = set()

related_by_source = Counter()
related_source_sessions = defaultdict(set)
related_by_target = Counter()
related_pair = Counter()
app_by_source = Counter()
app_source_sessions = defaultdict(set)
app_by_app = Counter()
app_by_target = Counter()
app_pair = Counter()
button_category = Counter()

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
            if event == "suggestion":
                scene_suggestion_sessions[source_scene].add(session)
            elif event == "chatResp":
                scene_chatresp_sessions[source_scene].add(session)
            elif event == "btnClick":
                button_total += 1
                scene_btn_sessions[source_scene].add(session)

                btn = item.get("btnName") or "(empty)"
                target = item.get("target") or ""
                app_name = item.get("appName") or ""

                if "pages/ChatH5" in target:
                    target_params = params_from_url(target)
                    target_scene = target_params.get("scene") or target_params.get("con_title") or target or "(empty)"
                    target_query = target_params.get("query") or target_params.get("con_title") or target_scene
                    related_clicks += 1
                    related_users.add(user)
                    related_sessions.add(session)
                    related_by_source[source_scene] += 1
                    related_source_sessions[source_scene].add(session)
                    related_by_target[(target_scene, target_query)] += 1
                    related_pair[(source_scene, target_scene, target_query)] += 1
                    button_category["关联阅读"] += 1
                elif btn == "jump_service" and app_name:
                    app_clicks += 1
                    app_users.add(user)
                    app_sessions.add(session)
                    app_by_source[source_scene] += 1
                    app_source_sessions[source_scene].add(session)
                    app_by_app[app_name] += 1
                    app_by_target[(app_name, target or "(empty)")] += 1
                    app_pair[(source_scene, app_name, target or "(empty)")] += 1
                    button_category["推荐应用"] += 1
                elif target.startswith("tel:"):
                    hotline_clicks += 1
                    button_category["电话跳转"] += 1
                else:
                    other_clicks += 1
                    button_category[btn] += 1


print("# 关联阅读与推荐应用分析")
print()
print("## 总览")
print(f"- 按钮点击总数: {button_total:,}")
print(f"- 关联阅读点击: {related_clicks:,}，用户 {len(related_users):,}，会话 {len(related_sessions):,}")
print(f"- 推荐应用点击: {app_clicks:,}，用户 {len(app_users):,}，会话 {len(app_sessions):,}")
print(f"- 电话跳转点击: {hotline_clicks:,}")
print(f"- 其他按钮点击: {other_clicks:,}")
print()

print("## 按钮类型")
print("| 类型 | 点击数 | 占按钮点击 |")
print("| --- | ---: | ---: |")
for key, count in button_category.most_common(TOP):
    print(f"| {text(key)} | {count:,} | {pct(count, button_total)} |")
print()

print("## 关联阅读来源场景")
print("| 来源场景 | 点击数 | 来源会话数 | 点击会话率 |")
print("| --- | ---: | ---: | ---: |")
for scene, count in related_by_source.most_common(TOP):
    base = len(scene_sessions[scene])
    print(f"| {text(scene)} | {count:,} | {base:,} | {pct(len(related_source_sessions[scene]), base)} |")
print()

print("## 关联阅读目标")
print("| 目标场景 | 目标 query | 点击数 |")
print("| --- | --- | ---: |")
for (target_scene, target_query), count in related_by_target.most_common(TOP):
    print(f"| {text(target_scene)} | {text(target_query)} | {count:,} |")
print()

print("## 关联阅读路径")
print("| 来源场景 | 目标场景 | 目标 query | 点击数 |")
print("| --- | --- | --- | ---: |")
for (source_scene, target_scene, target_query), count in related_pair.most_common(TOP):
    print(f"| {text(source_scene)} | {text(target_scene)} | {text(target_query)} | {count:,} |")
print()

print("## 推荐应用")
print("| 应用 | 点击数 | 占推荐应用点击 |")
print("| --- | ---: | ---: |")
for app_name, count in app_by_app.most_common(TOP):
    print(f"| {text(app_name)} | {count:,} | {pct(count, app_clicks)} |")
print()

print("## 推荐应用来源场景")
print("| 来源场景 | 点击数 | 来源会话数 | 点击会话率 |")
print("| --- | ---: | ---: | ---: |")
for scene, count in app_by_source.most_common(TOP):
    base = len(scene_sessions[scene])
    print(f"| {text(scene)} | {count:,} | {base:,} | {pct(len(app_source_sessions[scene]), base)} |")
print()

print("## 推荐应用目标")
print("| 应用 | target | 点击数 |")
print("| --- | --- | ---: |")
for (app_name, target), count in app_by_target.most_common(TOP):
    print(f"| {text(app_name)} | {text(target)} | {count:,} |")
print()

print("## 推荐应用路径")
print("| 来源场景 | 应用 | target | 点击数 |")
print("| --- | --- | --- | ---: |")
for (source_scene, app_name, target), count in app_pair.most_common(TOP):
    print(f"| {text(source_scene)} | {text(app_name)} | {text(target)} | {count:,} |")
