#!/usr/bin/env python3
"""指定日期的银行间国债发行额与到期额。

口径
    只统计在银行间市场上市的国债，避免同一只记账式国债在上交所 / 深交所 /
    银行间各记一次导致发行额被放大三倍。

数据来源（与 ChinaTreasury.ipynb 同一接口族，并补上发行明细）
    - 发行额: 巨潮资讯「国债发行」ak.bond_treasure_issue_cninfo。
      该接口按发行事件（含续发）给实际发行总量，且带「交易市场」字段；
      本脚本只保留交易市场含「银行间」的记录。
    - 到期额: 中国货币网债券信息（BondMarketInfoList2 + BondDetailInfo），
      债券类型 = 国债。该站本身就是银行间市场，每只券只出现一次；
      按到期兑付日汇总实际发行量（亿元）。

用法
    python treasury_ib_flow.py 2026-08-21
    python treasury_ib_flow.py --date 20260821 --refresh-cache
    python treasury_ib_flow.py 2026-08-21 --issue-on 缴款日
    python treasury_ib_flow.py --serve --port 8080
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pandas as pd
import requests
from requests.adapters import HTTPAdapter

try:
    import akshare as ak
except ImportError as exc:  # pragma: no cover
    raise SystemExit("需要 akshare：python3 -m pip install akshare pandas") from exc


CHINAMONEY_ORIGIN = "https://www.chinamoney.com.cn"
LIST_URL = f"{CHINAMONEY_ORIGIN}/ags/ms/cm-u-bond-md/BondMarketInfoList2"
DETAIL_URL = f"{CHINAMONEY_ORIGIN}/ags/ms/cm-u-bond-md/BondDetailInfo"
# 与 notebook 中 bond_type="国债" 对应的债券类型代码
TREASURY_BOND_TYPE = "100001"
IB_MARKET_KEYWORD = "银行间"
ROOT_DIR = Path(__file__).resolve().parent
CACHE_PATH = ROOT_DIR / ".cache" / "ib_treasury_details.csv"
HTML_PATH = ROOT_DIR / "treasury_ib_flow.html"
AMOUNT_UNIT = "亿元"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36"
    ),
    "Origin": CHINAMONEY_ORIGIN,
    "Referer": f"{CHINAMONEY_ORIGIN}/chinese/scsjzqxx/",
}

ISSUE_COLUMNS = [
    "债券代码",
    "债券简称",
    "债券名称",
    "发行起始日",
    "缴款日",
    "计划发行总量",
    "实际发行总量",
    "增发次数",
    "交易市场",
    "发行方式",
]
MATURITY_COLUMNS = [
    "债券代码",
    "债券简称",
    "债券全称",
    "发行日",
    "到期兑付日",
    "上市日",
    "期限",
    "计划发行量",
    "实际发行量",
]


def parse_date(text: str) -> pd.Timestamp:
    raw = str(text).strip().replace("/", "-")
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return pd.Timestamp(datetime.strptime(raw, fmt))
        except ValueError:
            continue
    raise ValueError(f"无法解析日期: {text!r}（支持 YYYY-MM-DD 或 YYYYMMDD）")


def _session() -> requests.Session:
    sess = requests.Session()
    sess.headers.update(HEADERS)
    adapter = HTTPAdapter(pool_connections=16, pool_maxsize=16, max_retries=0)
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    return sess


def _post(sess: requests.Session, url: str, data: dict, retries: int = 4) -> dict:
    last_err: Exception | None = None
    for attempt in range(retries):
        try:
            resp = sess.post(url, data=data, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except Exception as err:  # noqa: BLE001 - 远端接口偶发空响应/限流
            last_err = err
            time.sleep(0.6 * (attempt + 1))
    raise RuntimeError(f"请求失败 {url}: {last_err}") from last_err


def _to_amount(value) -> float:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return float("nan")
    text = str(value).strip().replace(",", "")
    if text in {"", "-", "---", "None", "nan"}:
        return float("nan")
    return float(text)


def fetch_ib_issuance(date: pd.Timestamp, issue_on: str) -> pd.DataFrame:
    """巨潮国债发行，只保留银行间市场记录。

    巨潮接口的起止日按发行起始日筛窗。缴款日通常晚于发行起始日，因此
    先多取一段窗口，再在本地按 ``issue_on`` 精确匹配查询日。
    """
    window_start = (date - pd.Timedelta(days=21)).strftime("%Y%m%d")
    window_end = (date + pd.Timedelta(days=3)).strftime("%Y%m%d")
    try:
        raw = ak.bond_treasure_issue_cninfo(
            start_date=window_start, end_date=window_end
        )
    except Exception:
        raw = pd.DataFrame()

    if raw is None or raw.empty:
        return pd.DataFrame(columns=ISSUE_COLUMNS)

    df = raw.copy()
    if "交易市场" not in df.columns:
        raise RuntimeError("巨潮国债发行接口未返回「交易市场」字段，无法做银行间过滤")

    df = df[df["交易市场"].astype(str).str.contains(IB_MARKET_KEYWORD, na=False)]
    date_col = issue_on
    if date_col not in df.columns:
        raise ValueError(f"发行日期字段不存在: {date_col}")

    df[date_col] = pd.to_datetime(df[date_col], errors="coerce").dt.normalize()
    df = df[df[date_col] == date.normalize()]
    df["实际发行总量"] = pd.to_numeric(df["实际发行总量"], errors="coerce")
    df["计划发行总量"] = pd.to_numeric(df.get("计划发行总量"), errors="coerce")
    keep = [c for c in ISSUE_COLUMNS if c in df.columns]
    return df[keep].sort_values(["债券代码", "债券简称"]).reset_index(drop=True)


def _list_ib_treasuries(sess: requests.Session) -> pd.DataFrame:
    """中国货币网国债列表（银行间），与 notebook 的 bond_info_cm(bond_type='国债') 相同。"""
    payload = {
        "pageNo": "1",
        "pageSize": "15",
        "bondName": "",
        "bondCode": "",
        "issueEnty": "",
        "bondType": TREASURY_BOND_TYPE,
        "bondSpclPrjctVrty": "",
        "couponType": "",
        "issueYear": "",
        "entyDefinedCode": "",
        "rtngShrt": "",
    }
    first = _post(sess, LIST_URL, payload)
    data = first.get("data") or {}
    page_total = int(data.get("pageTotal") or 1)
    rows = list(data.get("resultList") or [])

    def _page(page_no: int) -> list[dict]:
        body = dict(payload, pageNo=str(page_no))
        js = _post(sess, LIST_URL, body)
        return list((js.get("data") or {}).get("resultList") or [])

    if page_total > 1:
        with ThreadPoolExecutor(max_workers=8) as pool:
            futs = [pool.submit(_page, p) for p in range(2, page_total + 1)]
            for fut in as_completed(futs):
                rows.extend(fut.result())

    if not rows:
        return pd.DataFrame(columns=["查询代码", "债券简称", "债券代码", "发行日期"])
    frame = pd.DataFrame(rows)
    frame = frame.rename(
        columns={
            "bondDefinedCode": "查询代码",
            "bondName": "债券简称",
            "bondCode": "债券代码",
            "issueStartDate": "发行日期",
        }
    )
    return frame.drop_duplicates(subset=["查询代码"]).reset_index(drop=True)


def _detail_one(sess: requests.Session, defined_code: str) -> dict:
    js = _post(sess, DETAIL_URL, {"bondDefinedCode": defined_code})
    info = ((js.get("data") or {}).get("bondBaseInfo")) or {}
    return {
        "查询代码": defined_code,
        "债券代码": info.get("bondCode"),
        "债券简称": info.get("bondName"),
        "债券全称": info.get("bondFullName"),
        "发行日": info.get("issueDate"),
        "到期兑付日": info.get("mrtyDate"),
        "上市日": info.get("lstngDate"),
        "期限": info.get("bondPeriod"),
        "计划发行量": _to_amount(info.get("plndIssueAmnt")),
        "实际发行量": _to_amount(info.get("issueAmnt")),
        "债券类型": info.get("bondType"),
    }


def load_ib_treasury_details(
    refresh: bool, workers: int, quiet: bool = False
) -> pd.DataFrame:
    if CACHE_PATH.exists() and not refresh:
        cached = pd.read_csv(CACHE_PATH, dtype=str)
        for col in ("计划发行量", "实际发行量"):
            if col in cached.columns:
                cached[col] = pd.to_numeric(cached[col], errors="coerce")
        if not quiet:
            print(f"已读取缓存 {CACHE_PATH}（{len(cached)} 只国债）。需要刷新时加 --refresh-cache")
        return cached

    if not quiet:
        print("正在从中国货币网拉取银行间国债列表与详情（首次或刷新缓存）…")
    sess = _session()
    listing = _list_ib_treasuries(sess)
    if not quiet:
        print(f"列表 {len(listing)} 只，开始拉详情（workers={workers}）…")

    details: list[dict] = []
    codes = listing["查询代码"].tolist()
    done = 0
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(_detail_one, sess, code): code for code in codes}
        for fut in as_completed(futs):
            details.append(fut.result())
            done += 1
            if (done % 100 == 0 or done == len(codes)) and not quiet:
                elapsed = time.time() - t0
                print(f"  详情 {done}/{len(codes)}  {elapsed:.1f}s")

    df = pd.DataFrame(details)
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(CACHE_PATH, index=False)
    if not quiet:
        print(f"已写入缓存 {CACHE_PATH}")
    return df


def fetch_ib_maturity(
    date: pd.Timestamp, refresh: bool, workers: int, quiet: bool = False
) -> pd.DataFrame:
    details = load_ib_treasury_details(refresh=refresh, workers=workers, quiet=quiet)
    if details.empty:
        return pd.DataFrame(columns=MATURITY_COLUMNS)

    df = details.copy()
    df["到期兑付日"] = pd.to_datetime(df["到期兑付日"], errors="coerce").dt.normalize()
    df = df[df["到期兑付日"] == date.normalize()]
    df["实际发行量"] = pd.to_numeric(df.get("实际发行量"), errors="coerce")
    df["计划发行量"] = pd.to_numeric(df.get("计划发行量"), errors="coerce")
    keep = [c for c in MATURITY_COLUMNS if c in df.columns]
    return df[keep].sort_values(["债券代码", "债券简称"]).reset_index(drop=True)


def _fmt_amount(value: float) -> str:
    if pd.isna(value):
        return "0.00"
    return f"{float(value):,.2f}"


def _json_cell(value):
    if value is None:
        return None
    if isinstance(value, (pd.Timestamp,)):
        if pd.isna(value):
            return None
        return value.strftime("%Y-%m-%d")
    if hasattr(value, "item") and not isinstance(value, (bytes, str)):
        try:
            value = value.item()
        except Exception:
            pass
    if isinstance(value, float) and pd.isna(value):
        return None
    if hasattr(value, "strftime") and not isinstance(value, str):
        try:
            return value.strftime("%Y-%m-%d")
        except Exception:
            return str(value)
    return value


def dataframe_records(df: pd.DataFrame) -> list[dict]:
    if df is None or df.empty:
        return []
    records: list[dict] = []
    for row in df.to_dict(orient="records"):
        records.append({key: _json_cell(val) for key, val in row.items()})
    return records


def query_ib_flow(
    date: pd.Timestamp,
    issue_on: str = "发行起始日",
    refresh: bool = False,
    workers: int = 12,
    quiet: bool = False,
) -> dict:
    issuance = fetch_ib_issuance(date, issue_on=issue_on)
    maturity = fetch_ib_maturity(
        date, refresh=refresh, workers=workers, quiet=quiet
    )
    issue_amt = float(issuance["实际发行总量"].sum()) if not issuance.empty else 0.0
    mature_amt = float(maturity["实际发行量"].sum()) if not maturity.empty else 0.0
    return {
        "date": date.strftime("%Y-%m-%d"),
        "issue_on": issue_on,
        "unit": AMOUNT_UNIT,
        "scope": "仅银行间市场上市国债（跨市场不去重会重复加总）",
        "issuance_amount": round(issue_amt, 4),
        "maturity_amount": round(mature_amt, 4),
        "net_supply": round(issue_amt - mature_amt, 4),
        "issuance_count": int(len(issuance)),
        "maturity_count": int(len(maturity)),
        "issuance": dataframe_records(issuance),
        "maturity": dataframe_records(maturity),
    }


class TreasuryFlowHandler(SimpleHTTPRequestHandler):
    def log_message(self, fmt: str, *args) -> None:
        sys.stderr.write("%s - %s\n" % (self.address_string(), fmt % args))

    def _send(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path in {"/", "/index.html", "/treasury_ib_flow.html"}:
            if not HTML_PATH.exists():
                self._send(500, "找不到 treasury_ib_flow.html".encode("utf-8"), "text/plain; charset=utf-8")
                return
            self._send(200, HTML_PATH.read_bytes(), "text/html; charset=utf-8")
            return
        if parsed.path == "/api/query":
            params = parse_qs(parsed.query)
            raw_date = (params.get("date") or [""])[0]
            issue_on = (params.get("issue_on") or ["发行起始日"])[0]
            refresh = (params.get("refresh") or ["0"])[0] in {"1", "true", "yes"}
            if issue_on not in {"发行起始日", "缴款日"}:
                payload = {"error": "issue_on 只能是 发行起始日 或 缴款日"}
                self._send(
                    400,
                    json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                    "application/json; charset=utf-8",
                )
                return
            try:
                date = parse_date(raw_date)
                result = query_ib_flow(date, issue_on=issue_on, refresh=refresh, quiet=True)
                self._send(
                    200,
                    json.dumps(result, ensure_ascii=False, default=str).encode("utf-8"),
                    "application/json; charset=utf-8",
                )
            except ValueError as err:
                payload = {"error": str(err)}
                self._send(
                    400,
                    json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                    "application/json; charset=utf-8",
                )
            except Exception as err:  # noqa: BLE001
                payload = {"error": f"查询失败: {err}"}
                self._send(
                    500,
                    json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                    "application/json; charset=utf-8",
                )
            return
        self._send(404, "Not Found".encode("utf-8"), "text/plain; charset=utf-8")


def serve(host: str, port: int) -> None:
    httpd = ThreadingHTTPServer((host, port), TreasuryFlowHandler)
    print(f"国债发行/到期查询: http://{host}:{port}/")
    print("在浏览器中打开后输入日期即可查询。Ctrl+C 结束。")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n已停止。")
    finally:
        httpd.server_close()


def report(date: pd.Timestamp, issue_on: str, issuance: pd.DataFrame, maturity: pd.DataFrame) -> None:
    issue_amt = float(issuance["实际发行总量"].sum()) if not issuance.empty else 0.0
    mature_amt = float(maturity["实际发行量"].sum()) if not maturity.empty else 0.0
    net = issue_amt - mature_amt
    day = date.strftime("%Y-%m-%d")

    print()
    print("=" * 64)
    print(f"日期           {day}")
    print("统计口径       仅银行间市场上市国债（跨市场不去重会重复加总）")
    print(f"发行额匹配字段 {issue_on}")
    print(f"发行额         {_fmt_amount(issue_amt)} {AMOUNT_UNIT}  （{len(issuance)} 笔）")
    print(f"到期额         {_fmt_amount(mature_amt)} {AMOUNT_UNIT}  （{len(maturity)} 只）")
    print(f"净供给(发行-到期) {_fmt_amount(net)} {AMOUNT_UNIT}")
    print("=" * 64)

    print("\n【发行明细】来源: 巨潮资讯国债发行，交易市场含「银行间」")
    if issuance.empty:
        print("  （当日无银行间国债发行）")
    else:
        show = issuance.copy()
        with pd.option_context("display.max_columns", None, "display.width", 160):
            print(show.to_string(index=False))

    print("\n【到期明细】来源: 中国货币网 BondDetailInfo，债券类型=国债")
    if maturity.empty:
        print("  （当日无银行间国债到期）")
    else:
        show = maturity.copy()
        with pd.option_context("display.max_columns", None, "display.width", 160):
            print(show.to_string(index=False))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="拉取特定日期银行间市场国债的发行额与到期额"
    )
    parser.add_argument("date", nargs="?", help="查询日期，YYYY-MM-DD 或 YYYYMMDD")
    parser.add_argument("--date", dest="date_opt", help="查询日期（与位置参数二选一）")
    parser.add_argument(
        "--issue-on",
        choices=["发行起始日", "缴款日"],
        default="发行起始日",
        help="发行额按哪一列匹配日期。资金面分析可改用缴款日。",
    )
    parser.add_argument("--refresh-cache", action="store_true", help="强制刷新货币网国债详情缓存")
    parser.add_argument("--workers", type=int, default=12, help="拉详情时的并发数")
    parser.add_argument("--serve", action="store_true", help="启动网页，用浏览器按日期查询")
    parser.add_argument("--host", default="0.0.0.0", help="网页服务监听地址，默认 0.0.0.0")
    parser.add_argument("--port", type=int, default=8080, help="网页服务端口，默认 8080")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.serve:
        serve(args.host, args.port)
        return 0
    raw_date = args.date_opt or args.date
    if not raw_date:
        print(
            "请提供查询日期，例如: python treasury_ib_flow.py 2026-08-21\n"
            "或启动网页: python treasury_ib_flow.py --serve",
            file=sys.stderr,
        )
        return 2
    date = parse_date(raw_date)
    issuance = fetch_ib_issuance(date, issue_on=args.issue_on)
    maturity = fetch_ib_maturity(date, refresh=args.refresh_cache, workers=args.workers)
    report(date, args.issue_on, issuance, maturity)
    return 0


if __name__ == "__main__":
    sys.exit(main())
