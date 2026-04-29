import argparse
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from urllib.parse import parse_qs, urlparse


def percentile(values, pct):
    if not values:
        return None
    ordered = sorted(values)
    index = int(round((len(ordered) - 1) * pct))
    return ordered[index]


def hour_from_event(item):
    value = item.get("$time")
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc).astimezone().strftime("%Y-%m-%d %H:00")

    value = item.get("server_ts")
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / 1000, tz=timezone.utc).astimezone().strftime("%Y-%m-%d %H:00")

    return "unknown"


def day_from_hour(hour):
    if hour == "unknown":
        return "unknown"
    return hour[:10]


def query_params(url):
    if not url:
        return {}
    return {key: values[0] for key, values in parse_qs(urlparse(url).query).items() if values}


def user_key(item):
    return (
        item.get("userId")
        or item.get("distinct_id")
        or item.get("$device_id")
        or item.get("oaid")
        or item.get("uuid")
        or "unknown"
    )


def main():
    parser = argparse.ArgumentParser(description="Analyze JSONL dot logs.")
    parser.add_argument("root", nargs="?", default="20260407-09")
    parser.add_argument("--top", type=int, default=20)
    args = parser.parse_args()

    files = sorted(Path(args.root).rglob("*.log"))
    total_lines = 0
    bad_lines = 0

    events = Counter()
    by_hour = Counter()
    by_day = Counter()
    users_by_day = defaultdict(set)
    sessions_by_day = defaultdict(set)
    event_users = defaultdict(set)
    event_sessions = defaultdict(set)
    scene = Counter()
    app_version = Counter()
    html_version = Counter()
    os_counter = Counter()
    browser = Counter()
    device_type = Counter()
    host = Counter()
    pathname = Counter()
    referrer = Counter()
    input_type = Counter()
    resp_status = Counter()
    resp_type = Counter()
    query = Counter()
    detected_brand = Counter()
    btn_name = Counter()
    btn_target = Counter()
    btn_app = Counter()
    exit_type = Counter()
    exit_scroll_depth = Counter()
    page_scroll_depth = Counter()
    ip = Counter()
    user_events = Counter()
    session_events = Counter()
    user_sessions = defaultdict(set)
    session_first_event = {}
    session_events_seen = defaultdict(Counter)
    session_times = defaultdict(list)
    scene_sessions = defaultdict(set)
    scene_event_sessions = defaultdict(lambda: defaultdict(set))
    resp_latency = defaultdict(list)

    for path in files:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                total_lines += 1
                try:
                    item = json.loads(line)
                except json.JSONDecodeError:
                    bad_lines += 1
                    continue

                event = item.get("event", "unknown")
                hour = hour_from_event(item)
                day = day_from_hour(hour)
                user = user_key(item)
                session = item.get("$session_id") or "unknown"
                url_params = query_params(item.get("$current_url") or item.get("$session_entry_url"))

                events[event] += 1
                by_hour[hour] += 1
                by_day[day] += 1
                users_by_day[day].add(user)
                sessions_by_day[day].add(session)
                event_users[event].add(user)
                event_sessions[event].add(session)
                user_events[user] += 1
                session_events[session] += 1
                user_sessions[user].add(session)
                session_events_seen[session][event] += 1
                scene_name = item.get("scene") or url_params.get("scene") or "(empty)"
                scene_sessions[scene_name].add(session)
                scene_event_sessions[scene_name][event].add(session)

                if session not in session_first_event:
                    session_first_event[session] = event

                timestamp = item.get("$time")
                if isinstance(timestamp, (int, float)):
                    session_times[session].append(timestamp)

                value = item.get("respLatency")
                if isinstance(value, (int, float)):
                    resp_latency[event].append(value)

                scene[scene_name] += 1
                app_version[item.get("version") or url_params.get("app_version") or "(empty)"] += 1
                html_version[item.get("htmlVersion") or "(empty)"] += 1
                os_counter[item.get("$os") or "(empty)"] += 1
                browser[item.get("$browser") or "(empty)"] += 1
                device_type[item.get("$device_type") or "(empty)"] += 1
                host[item.get("$host") or "(empty)"] += 1
                pathname[item.get("$pathname") or "(empty)"] += 1
                referrer[item.get("$referring_domain") or item.get("$referrer") or "(empty)"] += 1
                input_type[item.get("inputType") or "(empty)"] += 1
                resp_status[item.get("respStatus") or "(empty)"] += 1
                resp_type[item.get("respType") or "(empty)"] += 1
                query[url_params.get("query") or "(empty)"] += 1
                detected_brand[url_params.get("detected_brand") or "(empty)"] += 1
                ip[item.get("ip") or "(empty)"] += 1

                if event == "btnClick":
                    btn_name[item.get("btnName") or "(empty)"] += 1
                    btn_target[item.get("target") or "(empty)"] += 1
                    btn_app[item.get("appName") or "(empty)"] += 1
                elif event == "exit":
                    exit_type[item.get("exitType") or "(empty)"] += 1
                    exit_scroll_depth[item.get("scrollDepth", "(empty)")] += 1
                elif event == "pageScroll":
                    page_scroll_depth[item.get("scrollDepth", "(empty)")] += 1

    all_users = set()
    for values in users_by_day.values():
        all_users.update(values)
    all_sessions = set()
    for values in sessions_by_day.values():
        all_sessions.update(values)

    completed_sessions = 0
    req_sessions = 0
    resp_success_sessions = 0
    suggestion_sessions = 0
    landing_sessions = 0
    for session, counter in session_events_seen.items():
        if counter.get("landingOnShow") or counter.get("landingRender"):
            landing_sessions += 1
        if counter.get("chatReq"):
            req_sessions += 1
        if counter.get("chatResp"):
            completed_sessions += 1
        if counter.get("chatResp") and counter.get("chatReq"):
            resp_success_sessions += 1
        if counter.get("suggestion"):
            suggestion_sessions += 1

    durations = []
    for times in session_times.values():
        if len(times) >= 2:
            durations.append(max(times) - min(times))

    repeat_users = sum(1 for sessions in user_sessions.values() if len(sessions) > 1)

    print("# 用户行为日志分析")
    print()
    print(f"- 文件数: {len(files)}")
    print(f"- 总事件行数: {total_lines:,}")
    print(f"- JSON 解析失败行数: {bad_lines:,}")
    print(f"- 用户数: {len(all_users):,}")
    print(f"- 会话数: {len(all_sessions):,}")
    print(f"- 人均事件数: {total_lines / len(all_users):.2f}" if all_users else "- 人均事件数: N/A")
    print(f"- 人均会话数: {sum(len(v) for v in user_sessions.values()) / len(user_sessions):.2f}" if user_sessions else "- 人均会话数: N/A")
    print(f"- 多会话用户数: {repeat_users:,}")
    print()

    print("## 每日概览")
    print("| 日期 | 事件数 | 用户数 | 会话数 | 人均事件 |")
    print("| --- | ---: | ---: | ---: | ---: |")
    for day in sorted(by_day):
        users = len(users_by_day[day])
        sessions = len(sessions_by_day[day])
        per_user = by_day[day] / users if users else 0
        print(f"| {day} | {by_day[day]:,} | {users:,} | {sessions:,} | {per_user:.2f} |")
    print()

    print("## 漏斗")
    print("| 阶段 | 会话数 | 占 landing 会话 |")
    print("| --- | ---: | ---: |")
    base = landing_sessions or 1
    for name, count in [
        ("landing", landing_sessions),
        ("chatReq", req_sessions),
        ("chatReq + chatResp", resp_success_sessions),
        ("chatResp", completed_sessions),
        ("suggestion", suggestion_sessions),
    ]:
        print(f"| {name} | {count:,} | {count / base:.2%} |")
    print()

    print("## 会话时长")
    if durations:
        print(f"- 中位数: {median(durations):.1f} 秒")
        print(f"- 平均值: {mean(durations):.1f} 秒")
        print(f"- P90: {percentile(durations, 0.90):.1f} 秒")
        print(f"- P95: {percentile(durations, 0.95):.1f} 秒")
    else:
        print("- 无法计算")
    print()

    sections = [
        ("事件分布", events),
        ("场景 Top", scene),
        ("入口 query Top", query),
        ("系统", os_counter),
        ("浏览器", browser),
        ("设备类型", device_type),
        ("App 版本", app_version),
        ("HTML 版本", html_version),
        ("来源域", referrer),
        ("detected_brand", detected_brand),
        ("btnName", btn_name),
        ("btn target", btn_target),
        ("btn appName", btn_app),
        ("exitType", exit_type),
        ("exit scrollDepth", exit_scroll_depth),
        ("pageScroll scrollDepth", page_scroll_depth),
        ("inputType", input_type),
        ("respStatus", resp_status),
        ("respType", resp_type),
        ("Path", pathname),
        ("Host", host),
    ]
    for title, counter in sections:
        print(f"## {title}")
        print("| 值 | 次数 | 占比 |")
        print("| --- | ---: | ---: |")
        subtotal = sum(counter.values()) or 1
        for key, count in counter.most_common(args.top):
            print(f"| {str(key).replace('|', '/')} | {count:,} | {count / subtotal:.2%} |")
        print()

    print("## 事件独立用户与会话")
    print("| 事件 | 事件数 | 用户数 | 会话数 |")
    print("| --- | ---: | ---: | ---: |")
    for event, count in events.most_common(args.top):
        print(f"| {event} | {count:,} | {len(event_users[event]):,} | {len(event_sessions[event]):,} |")
    print()

    print("## 响应延迟")
    print("| 事件 | 样本数 | 平均 ms | 中位 ms | P90 ms | P95 ms |")
    print("| --- | ---: | ---: | ---: | ---: | ---: |")
    for event, values in sorted(resp_latency.items()):
        print(
            f"| {event} | {len(values):,} | {mean(values):.1f} | {median(values):.1f} | "
            f"{percentile(values, 0.90):.1f} | {percentile(values, 0.95):.1f} |"
        )
    print()

    print("## 场景会话漏斗 Top")
    print("| 场景 | 会话数 | chatReq率 | chatResp率 | suggestion率 | btnClick率 | exit率 |")
    print("| --- | ---: | ---: | ---: | ---: | ---: | ---: |")
    for scene_name, sessions in sorted(scene_sessions.items(), key=lambda item: len(item[1]), reverse=True)[: args.top]:
        base_count = len(sessions) or 1
        scene_events = scene_event_sessions[scene_name]
        print(
            f"| {scene_name} | {base_count:,} | "
            f"{len(scene_events['chatReq']) / base_count:.2%} | "
            f"{len(scene_events['chatResp']) / base_count:.2%} | "
            f"{len(scene_events['suggestion']) / base_count:.2%} | "
            f"{len(scene_events['btnClick']) / base_count:.2%} | "
            f"{len(scene_events['exit']) / base_count:.2%} |"
        )
    print()

    print("## 小时趋势 Top")
    print("| 小时 | 事件数 |")
    print("| --- | ---: |")
    for hour, count in sorted(by_hour.items()):
        print(f"| {hour} | {count:,} |")


if __name__ == "__main__":
    main()
