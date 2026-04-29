import argparse
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlparse


DEFAULT_LOG_DIR = "logs"
DEFAULT_OUTPUT = "scene_click_rate_by_type.md"


def params_from_url(value):
    if not value:
        return {}
    parsed = urlparse(value)
    return {key: values[0] for key, values in parse_qs(parsed.query).items() if values}


def session_key(item):
    # Use conversationId for session counting
    return item.get("conversationId")


def scene_name(item):
    params = params_from_url(item.get("$current_url") or item.get("$session_entry_url"))
    return item.get("scene") or params.get("scene") or "(empty)"


def button_category(item):
    target = item.get("target") or ""
    btn = item.get("btnName") or "(empty)"
    app_name = item.get("appName") or ""

    if btn == "jump_service":
        low_target = target.lower()
        if ".pdf" in low_target:
            return "PDF下载"
        if "pages/ChatH5" in target:
            return "关联阅读"
        if "wxaurl.cn" in low_target:
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

    category_by_btn = {
        "up": "点赞",
        "barLike": "点赞",
        "down": "点踩",
        "barDislike": "点踩",
        "barComment": "评论入口",
        "copy": "复制",
        "regenerate": "重新生成",
        "expandTrace": "展开快递轨迹",
        "chatSend": "消息发送",
        "chatStop": "回复主动停止",
    }
    return category_by_btn.get(btn, btn)


def pct(value, base):
    return f"{value / base:.2%}" if base else "0.00%"


def clean(value):
    if value is None or value == "":
        return "(empty)"
    return str(value).replace("|", "/")


def event_day(item):
    value = item.get("$time")
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc).astimezone().strftime("%Y-%m-%d")
    return None


def event_timestamp(item):
    value = item.get("$time")
    if isinstance(value, (int, float)):
        return value
    return None


def file_day(path):
    match = re.search(r"(20\d{6})", str(path))
    if not match:
        return None
    value = match.group(1)
    return f"{value[:4]}-{value[4:6]}-{value[6:8]}"


def report_time_range(days):
    if not days:
        return "未知"
    start = min(days)
    end = max(days)
    if start == end:
        return start
    return f"{start} 至 {end}"


def avg(values):
    return sum(values) / len(values) if values else 0


def minutes(value):
    return f"{value:.2f}"


def scene_click_stats(category, scene, metrics):
    scene_sessions = metrics["scene_sessions"]
    type_scene_clicks = metrics["type_scene_clicks"]
    type_scene_sessions = metrics["type_scene_sessions"]
    type_scene_pv_clicks = metrics["type_scene_pv_clicks"]

    pv_clicks = type_scene_pv_clicks[category][scene]
    click_sessions = len(type_scene_sessions[category][scene])
    total_sessions = len(scene_sessions[scene])
    uv_rate = click_sessions / total_sessions if total_sessions else 0
    pv_rate = pv_clicks / total_sessions if total_sessions else 0
    return {
        "scene": scene,
        "clicks": type_scene_clicks[category][scene],
        "pv_clicks": pv_clicks,
        "click_sessions": click_sessions,
        "total_sessions": total_sessions,
        "uv_rate": uv_rate,
        "pv_rate": pv_rate,
    }


def sorted_scene_stats(category, metrics):
    stats = [
        scene_click_stats(category, scene, metrics)
        for scene in metrics["type_scene_clicks"][category]
    ]
    return sorted(
        stats,
        key=lambda item: (
            -item["clicks"],
            -item["click_sessions"],
            -item["pv_rate"],
            -item["uv_rate"],
            clean(item["scene"]),
        ),
    )


def sorted_scene_stay_stats(metrics):
    scene_sessions = metrics["scene_sessions"]
    scene_stay_minutes = metrics["scene_stay_minutes"]
    stats = []
    for scene, values in scene_stay_minutes.items():
        if scene == "(empty)" or not values:
            continue
        stats.append(
            {
                "scene": scene,
                "total_scene_sessions": len(scene_sessions[scene]),
                "exit_scene_sessions": len(values),
                "avg_stay_minutes": avg(values),
            }
        )
    return sorted(
        stats,
        key=lambda item: (
            -item["total_scene_sessions"],
            -item["exit_scene_sessions"],
            -item["avg_stay_minutes"],
            clean(item["scene"]),
        ),
    )


def collect_metrics(log_dir, pattern):
    scene_sessions = defaultdict(set)
    type_clicks = Counter()
    type_sessions = defaultdict(set)
    type_scene_clicks = defaultdict(Counter)
    type_scene_sessions = defaultdict(lambda: defaultdict(set))
    # 新增：按PV统计（不去重点击次数）
    type_scene_pv_clicks = defaultdict(Counter)
    event_days = set()
    file_days = set()
    first_entry_times = {}
    exit_times = {}

    files = sorted(Path(log_dir).rglob(pattern))
    total_lines = 0
    bad_lines = 0

    for path in files:
        day = file_day(path)
        if day:
            file_days.add(day)
        with path.open(encoding="utf-8", errors="replace") as handle:
            for line in handle:
                total_lines += 1
                try:
                    item = json.loads(line)
                except json.JSONDecodeError:
                    bad_lines += 1
                    continue

                session = session_key(item)
                if not session:
                    continue

                scene = scene_name(item)
                scene_sessions[scene].add(session)
                timestamp = event_timestamp(item)
                if timestamp is not None:
                    stay_key = (scene, session)
                    first_entry_times[stay_key] = min(timestamp, first_entry_times.get(stay_key, timestamp))
                    if item.get("event") == "exit":
                        exit_times[stay_key] = max(timestamp, exit_times.get(stay_key, timestamp))
                day = event_day(item)
                if day:
                    event_days.add(day)

                if item.get("event") != "btnClick":
                    continue

                category = button_category(item)
                type_clicks[category] += 1
                type_sessions[category].add(session)
                type_scene_clicks[category][scene] += 1
                type_scene_sessions[category][scene].add(session)
                # 新增：记录PV点击次数
                type_scene_pv_clicks[category][scene] += 1

    stay_minutes = []
    scene_stay_minutes = defaultdict(list)
    for key, exit_time in exit_times.items():
        if key not in first_entry_times or exit_time < first_entry_times[key]:
            continue
        scene, _session = key
        value = (exit_time - first_entry_times[key]) / 60
        stay_minutes.append(value)
        scene_stay_minutes[scene].append(value)

    return {
        "files": files,
        "total_lines": total_lines,
        "bad_lines": bad_lines,
        "scene_sessions": scene_sessions,
        "type_clicks": type_clicks,
        "type_sessions": type_sessions,
        "type_scene_clicks": type_scene_clicks,
        "type_scene_sessions": type_scene_sessions,
        "type_scene_pv_clicks": type_scene_pv_clicks,
        "days": file_days or event_days,
        "stay_minutes": stay_minutes,
        "scene_stay_minutes": scene_stay_minutes,
    }


def render_report(metrics, detail=True):
    scene_sessions = metrics["scene_sessions"]
    type_clicks = metrics["type_clicks"]
    type_sessions = metrics["type_sessions"]
    all_sessions = {session for sessions in scene_sessions.values() for session in sessions}
    ordered_types = [category for category, _ in type_clicks.most_common()]

    lines = [
        "# 各类型场景会话点击率",
        "",
        f"报告时间范围：{report_time_range(metrics['days'])}",
        "",
        "口径：",
        "- 场景会话点击率(UV) = 某类型在该场景的去重点击会话数 / 该场景总会话数",
        "- 场景会话点击率(PV) = 某类型在该场景的总点击次数 / 该场景总会话数",
        "- 页面平均停留时间 = exit 时间 - 用户首次进入场景页面时间，单位分钟；仅统计有 exit 的场景会话",
        "",
        f"- 日志文件数: {len(metrics['files']):,}",
        f"- 日志行数: {metrics['total_lines']:,}",
        f"- 解析失败行数: {metrics['bad_lines']:,}",
        f"- 全站去重会话数: {len(all_sessions):,}",
        f"- btnClick点击数: {sum(type_clicks.values()):,}",
        f"- 页面平均停留时间: {minutes(avg(metrics['stay_minutes']))} 分钟",
        f"- 停留时间样本数: {len(metrics['stay_minutes']):,}",
        "",
        "## 场景页面停留时间",
        "| 场景 | 场景会话总数 | 有exit的场景会话数 | 页面平均停留时间(分钟) |",
        "| --- | ---: | ---: | ---: |",
    ]

    for stats in sorted_scene_stay_stats(metrics):
        lines.append(
            f"| {clean(stats['scene'])} | {stats['total_scene_sessions']:,} | "
            f"{stats['exit_scene_sessions']:,} | "
            f"{minutes(stats['avg_stay_minutes'])} |"
        )

    lines.extend(
        [
        "",
        "## 类型汇总",
        "| 类型 | 点击数 | 点击会话数 | 主要来源场景 | 场景点击数 | 场景点击会话数 | 场景会话数 | 场景会话点击率(UV) | 场景会话点击率(PV) |",
        "| --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )

    for category in ordered_types:
        main_stats = sorted_scene_stats(category, metrics)[0]
        click_sessions = len(type_sessions[category])
        lines.append(
            f"| {clean(category)} | {type_clicks[category]:,} | {click_sessions:,} | "
            f"{clean(main_stats['scene'])} | "
            f"{main_stats['clicks']:,} | {main_stats['click_sessions']:,} | "
            f"{main_stats['total_sessions']:,} | {pct(main_stats['click_sessions'], main_stats['total_sessions'])} | "
            f"{pct(main_stats['pv_clicks'], main_stats['total_sessions'])} |"
        )

    if detail:
        lines.append("")
        for category in ordered_types:
            lines.extend(
                [
                    f"## {clean(category)}",
                    "| 来源场景 | 点击数 | 点击会话数 | 场景会话数 | 场景会话点击率(UV) | 场景会话点击率(PV) |",
                    "| --- | ---: | ---: | ---: | ---: | ---: |",
                ]
            )
            for stats in sorted_scene_stats(category, metrics):
                lines.append(
                    f"| {clean(stats['scene'])} | {stats['clicks']:,} | {stats['click_sessions']:,} | "
                    f"{stats['total_sessions']:,} | {pct(stats['click_sessions'], stats['total_sessions'])} | "
                    f"{pct(stats['pv_clicks'], stats['total_sessions'])} |"
                )
            lines.append("")

    return "\n".join(lines) + "\n"


def parse_args():
    parser = argparse.ArgumentParser(description="Generate scene session click-rate report by button type.")
    parser.add_argument(
        "--log-dir",
        default=DEFAULT_LOG_DIR,
        help=f"日志目录，支持递归读取。默认: {DEFAULT_LOG_DIR}",
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
        help=f"输出 Markdown 文件。默认: {DEFAULT_OUTPUT}",
    )
    parser.add_argument(
        "--pattern",
        default="*.log",
        help="日志文件匹配规则。默认: *.log",
    )
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="只输出类型汇总表，不输出每个类型的场景明细。",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    metrics = collect_metrics(args.log_dir, args.pattern)
    if not metrics["files"]:
        raise SystemExit(f"未找到日志文件: {Path(args.log_dir).resolve()} / {args.pattern}")

    report = render_report(metrics, detail=not args.summary_only)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(report, encoding="utf-8")

    print(f"已生成: {output.resolve()}")
    print(f"日志文件数: {len(metrics['files']):,}")
    print(f"btnClick点击数: {sum(metrics['type_clicks'].values()):,}")


if __name__ == "__main__":
    main()
