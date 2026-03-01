"""
refresh_all_points.py
---------------------
独立脚本：直接连接 MongoDB，并发刷新所有 Poe 账号的可用积分。
无需 FastAPI 服务运行。

用法
----
python refresh_all_points.py
python refresh_all_points.py --statuses active depleted cooldown
python refresh_all_points.py --concurrency 20
python refresh_all_points.py --env-file /path/to/.env.gateway
python refresh_all_points.py --dry-run        # 只列出账号，不实际刷新
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# 自动加载 .env.gateway / .env
# ---------------------------------------------------------------------------

def _load_dotenv(path: Path) -> bool:
    if not path.exists():
        return False
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if (value.startswith('"') and value.endswith('"')) or (
            value.startswith("'") and value.endswith("'")
        ):
            value = value[1:-1]
        os.environ.setdefault(key, value)
    return True


def _bootstrap_env(explicit_path: Optional[str] = None) -> None:
    if explicit_path:
        p = Path(explicit_path)
        if _load_dotenv(p):
            print(f"[env] 已加载配置文件: {p}")
            return
        else:
            print(f"[warn] 指定的配置文件不存在: {p}", file=sys.stderr)

    script_dir = Path(__file__).resolve().parent
    pkg_dir = script_dir / "poe_api_wrapper" / "openai"

    for candidate in (
        script_dir / ".env.gateway",
        script_dir / ".env",
        pkg_dir / ".env.gateway",
        pkg_dir / ".env",
    ):
        if _load_dotenv(candidate):
            print(f"[env] 已加载配置文件: {candidate}")
            break


# ---------------------------------------------------------------------------
# 主逻辑
# ---------------------------------------------------------------------------

async def run(
    statuses: list[str],
    concurrency: int,
    dry_run: bool,
    depleted_threshold: int,
) -> None:
    # 延迟导入，确保 env 已加载
    from poe_api_wrapper.openai.gateway import (
        AccountRepository,
        CredentialCrypto,
        PoeClientPool,
        mask_secret,
        utc_now,
    )

    def _require(name: str) -> str:
        v = os.environ.get(name, "").strip()
        if not v:
            print(f"[error] 缺少必要环境变量: {name}", file=sys.stderr)
            sys.exit(1)
        return v

    mongodb_uri = _require("MONGODB_URI")
    mongodb_db = _require("MONGODB_DB")
    fernet_key = _require("FERNET_KEY")
    poe_revision = os.environ.get("POE_REVISION", "").strip()

    print(f"[config] MongoDB: {mongodb_uri}  db={mongodb_db}")
    print(f"[config] 刷新状态范围: {statuses}")
    print(f"[config] 并发数: {concurrency}  耗尽阈值: {depleted_threshold}")
    print(f"[config] dry_run: {dry_run}")
    print("-" * 60)

    # 若未在环境变量中配置 POE_REVISION，则自动从 poe.com/login 页面抓取
    if not poe_revision:
        from poe_api_wrapper.openai.gateway import fetch_poe_revision
        print("[info] POE_REVISION 未配置，正在从 poe.com/login 自动获取...")
        poe_revision = await fetch_poe_revision() or ""
        if poe_revision:
            print(f"[info] 自动获取 poe-revision: {poe_revision}")
        else:
            print("[warn] 未能自动获取 poe-revision，将不携带该请求头")

    crypto = CredentialCrypto(fernet_key)
    repo = AccountRepository(mongodb_uri, mongodb_db, crypto)
    pool = PoeClientPool(repo=repo, default_poe_revision=poe_revision)

    try:
        # 查询目标账号
        all_docs = await repo.list_all_accounts_for_refresh(
            statuses=statuses,
            limit=5000,
        )
        total = len(all_docs)
        print(f"[info] 共找到 {total} 个账号需要刷新")

        if total == 0:
            print("[info] 没有符合条件的账号，退出。")
            return

        if dry_run:
            print("[dry-run] 以下账号将被刷新（仅列出，不实际操作）：")
            for doc in all_docs:
                print(f"  - {doc['_id']}")
            return

        # 并发刷新
        succeeded = 0
        failed = 0
        errors: list[dict] = []
        sem = asyncio.Semaphore(concurrency)
        lock = asyncio.Lock()

        async def _refresh_one(doc: dict) -> None:
            nonlocal succeeded, failed
            account_id = str(doc["_id"])
            async with sem:
                try:
                    account_doc = await repo.get_account_by_id(account_id)
                    if not account_doc:
                        raise RuntimeError("账号记录不存在")

                    client = await pool.get_client(account_doc)
                    settings = await client.get_settings()
                    message_info = settings.get("messagePointInfo", {})
                    subscription = settings.get("subscription", {})
                    balance = int(
                        message_info.get("subscriptionPointBalance", 0) or 0
                    ) + int(message_info.get("addonPointBalance", 0) or 0)
                    subscription_active = bool(subscription.get("isActive", False))

                    await repo.update_account_health(
                        account_id,
                        balance=balance,
                        subscription_active=subscription_active,
                        depleted_threshold=depleted_threshold,
                    )
                    await repo.mark_account_success(account_id)

                    status_tag = "depleted" if balance <= depleted_threshold else "active"
                    async with lock:
                        succeeded += 1
                        done = succeeded + failed
                        print(
                            f"  [{done}/{total}] {mask_secret(account_id, 8)} "
                            f"积分={balance}  订阅={subscription_active}  状态={status_tag}"
                        )
                except Exception as exc:
                    async with lock:
                        failed += 1
                        done = succeeded + failed
                        err_msg = str(exc)
                        errors.append({"account_id": account_id, "error": err_msg})
                        print(
                            f"  [{done}/{total}] {mask_secret(account_id, 8)} "
                            f"[FAILED] {err_msg[:120]}"
                        )

        print(f"[info] 开始刷新（并发={concurrency}）...")
        start_at = utc_now()
        await asyncio.gather(*(_refresh_one(doc) for doc in all_docs))
        elapsed = (utc_now() - start_at).total_seconds()

        print("-" * 60)
        print(f"[done] 耗时 {elapsed:.1f}s")
        print(f"       总计={total}  成功={succeeded}  失败={failed}")
        if errors:
            print(f"[errors] 前 {min(len(errors), 20)} 条失败详情：")
            for e in errors[:20]:
                print(f"  account_id={e['account_id']}  error={e['error']}")

    finally:
        await pool.close_all()
        await repo.close()


# ---------------------------------------------------------------------------
# CLI 入口
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="刷新所有 Poe 账号的可用积分（直连 MongoDB，无需服务运行）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--statuses",
        nargs="+",
        default=["active", "depleted", "cooldown", "invalid"],
        metavar="STATUS",
        help="要刷新的账号状态，空格分隔（默认：active depleted cooldown invalid）",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=10,
        metavar="N",
        help="并发刷新账号数量（默认：10，上限：50）",
    )
    parser.add_argument(
        "--depleted-threshold",
        type=int,
        default=None,
        metavar="N",
        help="积分低于此值视为耗尽（默认读取 DEPLETED_THRESHOLD 环境变量，fallback=20）",
    )
    parser.add_argument(
        "--env-file",
        default=None,
        metavar="PATH",
        help="手动指定 .env.gateway 文件路径",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="只列出账号，不实际调用 Poe API",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    _bootstrap_env(args.env_file)

    concurrency = max(1, min(args.concurrency, 50))

    depleted_threshold = args.depleted_threshold
    if depleted_threshold is None:
        try:
            depleted_threshold = int(os.environ.get("DEPLETED_THRESHOLD", "20"))
        except ValueError:
            depleted_threshold = 20

    asyncio.run(
        run(
            statuses=args.statuses,
            concurrency=concurrency,
            dry_run=args.dry_run,
            depleted_threshold=depleted_threshold,
        )
    )


if __name__ == "__main__":
    main()
