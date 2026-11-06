#!/usr/bin/env python3
"""按 cases_list.md 下载挑战 case（每个 case 对应 OBS 根目录下的同名目录）。

运行环境：Python 3.8+，实际下载需要安装华为云 MoXing。
认证：预先设置 ACCESS_KEY_ID、SECRET_ACCESS_KEY 环境变量。
可选：S3_ENDPOINT、S3_USE_HTTPS（默认 1）。不在脚本中存储密钥。

示例（Linux / ModelArts）：
    python challenge_cases_download.py --dry-run
    python challenge_cases_download.py
    python challenge_cases_download.py --source 839cases --case 016_2 027_2
    python challenge_cases_download.py --output /your/data/challenge_cases

保存结构：<output>/<数据来源>/<数据名称>/，保留 case 内部目录结构。
重复运行会跳过带完成标记的 case；未完成的 case 会重新复制（不是字节级续传）。
如需更新已下载的数据或修复本地文件，请使用 --force。
"""

import argparse
from collections import Counter
import json
import os
from pathlib import Path
import re
import sys
import time
from urllib.parse import urlsplit


OBS_ROOTS = {
    "839cases": "obs://yw-2030-gy/external/personal/x30080119/wenti_rosbag_decoded/",
    "191cases": "obs://yw-2030-gy/external/personal/x30080119/parking_data/rosbag_analysis_data/2030lab_141cases/",
}
DEFAULT_OUTPUT = "/cache/wx1557117/data/yw2030gy/challenge_cases/"
DEFAULT_ENDPOINT = "https://obs.cn-southwest-2.myhuaweicloud.com"
MARKER_NAME = ".challenge_download_complete.json"


def read_cases(path):
    """按表头读取 Markdown 表格，跳过空行，按来源和名称去重并保序。"""
    cases, seen = [], set()
    columns = None
    for line_number, line in enumerate(Path(path).read_text(encoding="utf-8-sig").splitlines(), 1):
        if not line.lstrip().startswith("|"):
            continue
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if "数据来源" in cells and "数据名称" in cells:
            columns = (cells.index("数据来源"), cells.index("数据名称"))
            continue
        if columns is None or all(not cell or re.fullmatch(r":?-+:?", cell) for cell in cells):
            continue
        if len(cells) <= max(columns):
            raise ValueError("清单第 {} 行缺少数据来源或数据名称列".format(line_number))
        source, name = (cells[i] for i in columns)
        if not source and not name:
            continue
        if source not in OBS_ROOTS:
            raise ValueError("清单第 {} 行的数据来源不支持：{!r}".format(line_number, source))
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", name):
            raise ValueError("清单第 {} 行的数据名称无效：{!r}".format(line_number, name))
        case = (source, name)
        if case not in seen:
            seen.add(case)
            cases.append(case)
    if not cases:
        raise ValueError("清单中未找到有效 case，请检查“数据来源”和“数据名称”表头")
    return cases


def configure_moxing():
    missing = [key for key in ("ACCESS_KEY_ID", "SECRET_ACCESS_KEY") if not os.environ.get(key, "").strip()]
    if missing:
        raise ValueError("请先设置认证环境变量：{}".format(", ".join(missing)))
    endpoint = os.environ.get("S3_ENDPOINT", DEFAULT_ENDPOINT).strip()
    secure = os.environ.get("S3_USE_HTTPS", "1").strip()
    if secure not in ("0", "1"):
        raise ValueError("S3_USE_HTTPS 只能为 0 或 1")
    parsed = urlsplit(endpoint if "://" in endpoint else "https://" + endpoint)
    if (parsed.scheme not in ("http", "https") or not parsed.hostname
            or parsed.username or parsed.password or parsed.path not in ("", "/")
            or parsed.query or parsed.fragment):
        raise ValueError("S3_ENDPOINT 必须是有效的 OBS 服务域名，不含桶名或对象路径")
    try:
        import moxing as mox
    except ImportError as exc:
        raise RuntimeError("当前环境缺少 MoXing，请在已安装华为云 MoXing 的环境运行；--dry-run 不需要它") from exc
    mox.file.set_auth(
        ak=os.environ["ACCESS_KEY_ID"],
        sk=os.environ["SECRET_ACCESS_KEY"],
        server=parsed.netloc,
        is_secure=(secure == "1"),
        ssl_verify=True,
    )
    return mox


def write_json(path, data):
    """先写临时文件再替换，避免中断时留下半份完成标记或汇总。"""
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(path)


def is_complete(destination, obs_path):
    try:
        marker = json.loads((destination / MARKER_NAME).read_text(encoding="utf-8"))
        return marker.get("obs_path") == obs_path and any(
            p.name != MARKER_NAME for p in destination.iterdir()
        )
    except (OSError, ValueError, AttributeError):
        return False


def download_case(mox, source, name, output, retries=2, force=False):
    obs_path = OBS_ROOTS[source].rstrip("/") + "/" + name + "/"
    destination = output / source / name
    result = {"source": source, "name": name, "obs_path": obs_path, "local_path": str(destination)}
    if not force and is_complete(destination, obs_path):
        return dict(result, status="skipped", attempts=0)

    for attempt in range(1, retries + 2):
        try:
            # 使用目录边界，避免把 016_2 与 016_20 等相似名称混在一起。
            if not mox.file.is_directory(obs_path):
                raise FileNotFoundError("OBS case 目录不存在或不是目录：" + obs_path)
            if not mox.file.list_directory(obs_path):
                raise ValueError("OBS case 目录为空：" + obs_path)
            destination.mkdir(parents=True, exist_ok=True)
            marker = destination / MARKER_NAME
            # 重下前撤销旧标记，防止本次失败后误认为已完成。
            if marker.exists():
                marker.unlink()
            mox.file.copy_parallel(obs_path, str(destination))
            if not any(p.name != MARKER_NAME for p in destination.iterdir()):
                raise RuntimeError("复制结束后目标目录仍为空")
            write_json(marker, {"obs_path": obs_path, "completed_at": time.strftime("%Y-%m-%dT%H:%M:%S%z")})
            return dict(result, status="downloaded", attempts=attempt)
        except Exception as exc:
            error = "{}: {}".format(type(exc).__name__, exc)
            print("  尝试 {}/{} 失败：{}".format(attempt, retries + 1, error), flush=True)
            if attempt <= retries:
                time.sleep(min(2 ** (attempt - 1), 10))
    return dict(result, status="failed", attempts=retries + 1, error=error)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--cases-list", type=Path, default=Path(__file__).resolve().with_name("cases_list.md"))
    parser.add_argument("--output", type=Path, default=Path(DEFAULT_OUTPUT), help="本地下载根目录")
    parser.add_argument("--source", nargs="+", choices=list(OBS_ROOTS), help="只下载指定来源")
    parser.add_argument("--case", nargs="+", dest="case_names", help="只下载指定名称，名称须在清单中")
    parser.add_argument("--dry-run", action="store_true", help="仅打印计划，不连接 OBS、不创建文件")
    parser.add_argument("--retries", type=int, default=2, help="每个 case 失败后的重试次数（默认 2）")
    parser.add_argument("--force", action="store_true", help="重新复制已完成的 case，覆盖同名文件；不清理额外本地文件")
    args = parser.parse_args(argv)
    if args.retries < 0:
        parser.error("--retries 不能小于 0")
    try:
        cases = read_cases(args.cases_list)
        if args.source:
            cases = [case for case in cases if case[0] in args.source]
        if args.case_names:
            unknown = set(args.case_names) - {name for _, name in cases}
            if unknown:
                raise ValueError("指定来源/清单中没有这些 case：" + ", ".join(sorted(unknown)))
            cases = [case for case in cases if case[1] in args.case_names]
        if not cases:
            raise ValueError("筛选后没有可下载的 case")
    except (OSError, ValueError) as exc:
        parser.error(str(exc))

    counts = Counter(source for source, _ in cases)
    print("共 {} 个 case：{}".format(len(cases), dict(counts)), flush=True)
    if args.dry_run:
        for source, name in cases:
            print("{}/{}/ -> {}".format(OBS_ROOTS[source].rstrip("/"), name, args.output / source / name))
        print("以上仅为计划，尚未验证 OBS 目录是否存在。")
        return 0

    try:
        mox = configure_moxing()
        args.output.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        print("初始化失败：{}".format(exc), file=sys.stderr)
        return 1

    results = []
    summary_path = args.output / "download_summary.json"
    for index, (source, name) in enumerate(cases, 1):
        print("[{}/{}] {}/{}".format(index, len(cases), source, name), flush=True)
        results.append(download_case(mox, source, name, args.output, args.retries, args.force))
        print("  {}".format(results[-1]["status"]), flush=True)
        write_json(summary_path, results)
    counts = Counter(item["status"] for item in results)
    print("完成：下载 {}，跳过 {}，失败 {}。汇总：{}".format(
        counts["downloaded"], counts["skipped"], counts["failed"], summary_path))
    return 1 if counts["failed"] else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n已中断；重新执行时将跳过已完成的 case。", file=sys.stderr)
        sys.exit(130)
