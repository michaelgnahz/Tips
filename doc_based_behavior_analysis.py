import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from urllib.parse import parse_qs, unquote, urlparse


ROOT = Path("20260407-09")
TOP = 20


def params_from_url(value):
    if not value:
        return {}
    parsed = urlparse(value)
    return {key: values[0] for key, values in parse_qs(parsed.query).items() if values}


def user_key(item):
    # userId is the documented quick-app user identifier. Fall back only for malformed/test rows.
    return item.get("userId") or item.get("randomId") or item.get("oaid") or item.get("$device_id") or "unknown"


def session_key(item):
    # The doc names conversationId as open-close identifier, but landing/chatReq often have it empty.
    # SDK session_id is present across the full open path, so use it for funnel/session metrics.
    return item.get("$session_id") or item.get("conversationId") or "unknown"


def hour_and_day(item):
    value = item.get("$time")
    if isinstance(value, (int, float)):
        hour = datetime.fromtimestamp(value, tz=timezone.utc).astimezone().strftime("%Y-%m-%d %H:00")
        return hour, hour[:10]
    return "unknown", "unknown"


def pct(value, base):
    return f"{value / base:.2%}" if base else "0.00%"


def percentile(values, p):
    if not values:
        return None
    values = sorted(values)
    return values[round((len(values) - 1) * p)]


def clean(value):
    if value is None or value == "":
        return "(empty)"
    return str(value).replace("|", "/")


def target_name(target):
    if not target:
        return "(empty)"
    parsed = urlparse(target)
    if parsed.scheme in {"http", "https"}:
        name = unquote(parsed.path.rsplit("/", 1)[-1])
        return name or target
    if "pages/ChatH5" in target:
        params = params_from_url(target)
        return params.get("query") or params.get("con_title") or params.get("scene") or target
    return target


def btn_category(item):
    target = item.get("target") or ""
    btn = item.get("btnName") or "(empty)"
    app_name = item.get("appName") or ""
    if btn == "jump_service":
        low = target.lower()
        if ".pdf" in low:
            return "PDF下载"
        if "pages/ChatH5" in target:
            return "关联阅读"
        if "wxaurl.cn" in low:
            return "相关推荐"
        if target.startswith("weixin:"):
            return "关联阅读微信"
        if target.startswith("tel:"):
            return "电话跳转"
        if app_name:
            return "推荐应用"
        if target:
            return "外部链接/其他热区"
        return "服务跳转未知"
    if btn in {"up", "barLike"}:
        return "点赞"
    if btn in {"down", "barDislike"}:
        return "点踩"
    if btn == "barComment":
        return "评论入口"
    if btn == "copy":
        return "复制"
    if btn == "regenerate":
        return "重新生成"
    if btn == "expandTrace":
        return "展开快递轨迹"
    if btn == "chatSend":
        return "消息发送"
    if btn == "chatStop":
        return "回复主动停止"
    return btn


files = sorted(ROOT.rglob("*.log"))
total = 0
bad = 0
events = Counter()
events_by_day = Counter()
events_by_hour = Counter()
users = set()
sessions = set()
users_by_day = defaultdict(set)
sessions_by_day = defaultdict(set)
session_events = defaultdict(Counter)
session_times = defaultdict(list)
session_scene = {}
scene_sessions = defaultdict(set)
scene_events = defaultdict(lambda: defaultdict(set))

conversation_empty_by_event = Counter()
conversation_total_by_event = Counter()
scene = Counter()
query = Counter()
app_version = Counter()
html_version = Counter()
brand = Counter()
input_type = Counter()
resp_status = Counter()
resp_type = Counter()
exit_type = Counter()
exit_scroll = Counter()
scroll_depth = Counter()
btn_name = Counter()
btn_cat = Counter()
btn_target = Counter()
btn_category_scene = defaultdict(Counter)
target_detail_by_category = defaultdict(Counter)
latency_by_event = defaultdict(list)
landing_token_latency = []

for path in files:
    with path.open(encoding="utf-8", errors="replace") as handle:
        for line in handle:
            total += 1
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                bad += 1
                continue

            event = item.get("event") or "unknown"
            user = user_key(item)
            session = session_key(item)
            params = params_from_url(item.get("$current_url") or item.get("$session_entry_url"))
            scene_name = item.get("scene") or params.get("scene") or "(empty)"
            source_query = params.get("query") or "(empty)"
            hour, day = hour_and_day(item)

            events[event] += 1
            events_by_day[day] += 1
            events_by_hour[hour] += 1
            users.add(user)
            sessions.add(session)
            users_by_day[day].add(user)
            sessions_by_day[day].add(session)
            session_events[session][event] += 1
            session_scene.setdefault(session, scene_name)
            scene_sessions[scene_name].add(session)
            scene_events[scene_name][event].add(session)
            users_by_day[day].add(user)

            timestamp = item.get("$time")
            if isinstance(timestamp, (int, float)):
                session_times[session].append(timestamp)

            conversation_total_by_event[event] += 1
            if not item.get("conversationId"):
                conversation_empty_by_event[event] += 1

            scene[scene_name] += 1
            query[source_query] += 1
            app_version[item.get("version") or params.get("app_version") or "(empty)"] += 1
            html_version[item.get("htmlVersion") or "(empty)"] += 1
            brand[params.get("detected_brand") or "(empty)"] += 1

            value = item.get("respLatency")
            if isinstance(value, (int, float)):
                latency_by_event[event].append(value)
            token_value = item.get("landingTokenLatency")
            if isinstance(token_value, (int, float)):
                landing_token_latency.append(token_value)

            if event == "chatReq":
                input_type[item.get("inputType") or "(empty)"] += 1
            elif event in {"chatResp", "suggestion"}:
                resp_status[(event, item.get("respStatus") or "(empty)")] += 1
                resp_type[(event, item.get("respType") or "(empty)")] += 1
            elif event == "exit":
                exit_type[item.get("exitType") or "(empty)"] += 1
                exit_scroll[item.get("scrollDepth", "(empty)")] += 1
            elif event == "pageScroll":
                scroll_depth[item.get("scrollDepth", "(empty)")] += 1
            elif event == "btnClick":
                btn = item.get("btnName") or "(empty)"
                cat = btn_category(item)
                target = item.get("target") or "(empty)"
                btn_name[btn] += 1
                btn_cat[cat] += 1
                btn_target[target] += 1
                btn_category_scene[cat][scene_name] += 1
                target_detail_by_category[cat][target_name(target)] += 1


durations = [max(v) - min(v) for v in session_times.values() if len(v) > 1]
landing_sessions = {s for s, c in session_events.items() if c.get("landingOnShow")}
render_sessions = {s for s, c in session_events.items() if c.get("landingRender")}
req_sessions = {s for s, c in session_events.items() if c.get("chatReq")}
resp_sessions = {s for s, c in session_events.items() if c.get("chatResp")}
success_resp_sessions = {
    s for s, c in session_events.items() if c.get("chatReq") and c.get("chatResp")
}
suggestion_sessions = {s for s, c in session_events.items() if c.get("suggestion")}
exit_sessions = {s for s, c in session_events.items() if c.get("exit")}
btn_sessions = {s for s, c in session_events.items() if c.get("btnClick")}
scroll_sessions = {s for s, c in session_events.items() if c.get("pageScroll")}

print("# 按埋点文档重算的用户行为分析")
print()
print("## 文档口径")
print("- `landingOnShow`: 首轮加载；`landingRender`: 首轮回复渲染完成。")
print("- `chatReq`: 发送请求，`inputType` 分为 preSet/text/suggest。")
print("- `chatResp`: 收到回复，按 `respStatus/respType/respLatency/landingTokenLatency` 分析。")
print("- `suggestion`: 下一步建议，按 `respStatus/respType/respLatency` 分析。")
print("- `btnClick`: 按 `btnName + target` 区分 PDF 下载、关联阅读、相关推荐、推荐应用、电话跳转等热区。")
print("- `exit`: 按 `exitType` 区分生成中退出、生成后退出、外链跳转退出、其他退出。")
print()

print("## 数据概览")
print(f"- 文件数: {len(files)}")
print(f"- 事件数: {total:,}")
print(f"- 解析失败: {bad:,}")
print(f"- userId口径用户数: {len(users):,}")
print(f"- session_id口径会话数: {len(sessions):,}")
print(f"- 人均事件数: {total / len(users):.2f}" if users else "- 人均事件数: 0")
print(f"- 会话均事件数: {total / len(sessions):.2f}" if sessions else "- 会话均事件数: 0")
print()

print("## 每日趋势")
print("| 日期 | 事件数 | 用户数 | 会话数 |")
print("| --- | ---: | ---: | ---: |")
for day in sorted(events_by_day):
    print(f"| {day} | {events_by_day[day]:,} | {len(users_by_day[day]):,} | {len(sessions_by_day[day]):,} |")
print()

print("## 核心链路漏斗")
base = len(landing_sessions) or 1
print("| 阶段 | 会话数 | 占首轮加载会话 |")
print("| --- | ---: | ---: |")
for name, value in [
    ("首轮加载 landingOnShow", len(landing_sessions)),
    ("首轮渲染 landingRender", len(render_sessions)),
    ("发送请求 chatReq", len(req_sessions)),
    ("请求且收到回复", len(success_resp_sessions)),
    ("下一步建议 suggestion", len(suggestion_sessions)),
    ("页面滚动 pageScroll", len(scroll_sessions)),
    ("按钮点击 btnClick", len(btn_sessions)),
    ("退出 exit", len(exit_sessions)),
]:
    print(f"| {name} | {value:,} | {pct(value, base)} |")
print()

print("## 会话时长")
if durations:
    print(f"- 中位数: {median(durations):.1f} 秒")
    print(f"- 平均值: {mean(durations):.1f} 秒")
    print(f"- P90: {percentile(durations, 0.9):.1f} 秒")
    print(f"- P95: {percentile(durations, 0.95):.1f} 秒")
print()

def print_counter(title, counter, total_count=None):
    print(f"## {title}")
    print("| 值 | 次数 | 占比 |")
    print("| --- | ---: | ---: |")
    total_value = total_count if total_count is not None else sum(counter.values())
    for key, count in counter.most_common(TOP):
        print(f"| {clean(key)} | {count:,} | {pct(count, total_value)} |")
    print()

print_counter("事件分布", events, total)
print_counter("场景 Top", scene, total)
print_counter("入口 Query Top", query, total)
print_counter("chatReq 输入类型", input_type)

print("## chatResp / suggestion 状态")
print("| 事件 | 状态 | 次数 |")
print("| --- | --- | ---: |")
for (event, status), count in resp_status.most_common(TOP):
    print(f"| {event} | {clean(status)} | {count:,} |")
print()

print("## chatResp / suggestion 类型")
print("| 事件 | 类型 | 次数 |")
print("| --- | --- | ---: |")
for (event, rtype), count in resp_type.most_common(TOP):
    print(f"| {event} | {clean(rtype)} | {count:,} |")
print()

print("## 响应耗时")
print("| 事件 | 样本数 | 平均ms | 中位ms | P90ms | P95ms |")
print("| --- | ---: | ---: | ---: | ---: | ---: |")
for event, values in sorted(latency_by_event.items()):
    print(f"| {event} | {len(values):,} | {mean(values):.1f} | {median(values):.1f} | {percentile(values, 0.9):.1f} | {percentile(values, 0.95):.1f} |")
if landing_token_latency:
    values = landing_token_latency
    print(f"| landingTokenLatency | {len(values):,} | {mean(values):.1f} | {median(values):.1f} | {percentile(values, 0.9):.1f} | {percentile(values, 0.95):.1f} |")
print()

print_counter("按钮名称", btn_name)
print_counter("按钮/热区分类", btn_cat)
for category in ["PDF下载", "关联阅读", "相关推荐", "推荐应用", "电话跳转", "展开快递轨迹", "点赞", "点踩", "评论入口"]:
    if target_detail_by_category[category]:
        print_counter(f"{category}目标", target_detail_by_category[category], sum(target_detail_by_category[category].values()))
        print_counter(f"{category}来源场景", btn_category_scene[category], sum(btn_category_scene[category].values()))

print_counter("退出类型", exit_type)
print_counter("退出滚动深度", exit_scroll)
print_counter("页面滚动深度", scroll_depth)
print_counter("App版本", app_version, total)
print_counter("HTML版本", html_version, total)
print_counter("品牌", brand, total)

print("## 场景链路 Top")
print("| 场景 | 会话数 | chatReq率 | chatResp率 | suggestion率 | btnClick率 | exit率 |")
print("| --- | ---: | ---: | ---: | ---: | ---: | ---: |")
for scene_name, scene_session_set in sorted(scene_sessions.items(), key=lambda item: len(item[1]), reverse=True)[:TOP]:
    scene_base = len(scene_session_set)
    ev = scene_events[scene_name]
    print(
        f"| {clean(scene_name)} | {scene_base:,} | {pct(len(ev['chatReq']), scene_base)} | "
        f"{pct(len(ev['chatResp']), scene_base)} | {pct(len(ev['suggestion']), scene_base)} | "
        f"{pct(len(ev['btnClick']), scene_base)} | {pct(len(ev['exit']), scene_base)} |"
    )
print()

print("## conversationId 完整性")
print("| 事件 | 总数 | conversationId为空 | 空值占比 |")
print("| --- | ---: | ---: | ---: |")
for event, count in conversation_total_by_event.most_common(TOP):
    empty = conversation_empty_by_event[event]
    print(f"| {event} | {count:,} | {empty:,} | {pct(empty, count)} |")
