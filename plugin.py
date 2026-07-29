"""IronClaw Bridge — 为 Planner 提供远程自主 Agent 异步任务委托能力。

通过 SSH 向 baremetal3 上的 IronClaw 提交任务，后台轮询结果，
完成后用 maisaka.proactive.trigger 唤醒 Planner，注入任务快照 + 结果。
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from maibot_sdk import Field, MaiBotPlugin, PluginConfigBase, Tool
from maibot_sdk.types import ToolParameterInfo, ToolParamType

logger = logging.getLogger(__name__)

# ── config ──────────────────────────────────────────────────────────────


class PluginSectionConfig(PluginConfigBase):
    __ui_label__ = "插件"
    __ui_icon__ = "package"
    __ui_order__ = 0

    enabled: bool = Field(default=True, description="是否启用插件")
    config_version: str = Field(default="1.0.0", description="配置版本")


class IronClawSectionConfig(PluginConfigBase):
    __ui_label__ = "IronClaw"
    __ui_icon__ = "bot"
    __ui_order__ = 1

    ssh_host: str = Field(default="baremetal3.agents.near.ai", description="SSH 主机")
    ssh_port: int = Field(default=19536, description="SSH 端口")
    ssh_user: str = Field(default="agent", description="SSH 用户")
    ssh_key: str = Field(default="", description="SSH 密钥路径，空=系统默认")
    remote_workspace: str = Field(default="/home/agent/workspace", description="远端工作目录")
    remote_results_dir: str = Field(default="/home/agent/ironclaw-results", description="远端结果目录")
    task_timeout: int = Field(default=600, description="任务超时秒数")
    max_concurrent: int = Field(default=3, description="最大并发任务")
    poll_interval: int = Field(default=15, description="轮询间隔秒")
    max_result_chars: int = Field(default=4000, description="结果截断长度")
    snapshot_chat_count: int = Field(default=8, description="快照保存群聊条数")


class IronClawConfig(PluginConfigBase):
    plugin: PluginSectionConfig = Field(default_factory=PluginSectionConfig)
    ironclaw: IronClawSectionConfig = Field(default_factory=IronClawSectionConfig)


# ── helpers ──────────────────────────────────────────────────────────────


def _tool_param(
    name: str,
    param_type: ToolParamType,
    description: str,
    required: bool = False,
) -> ToolParameterInfo:
    return ToolParameterInfo(
        name=name,
        param_type=param_type,
        description=description,
        required=required,
    )


def _project_root() -> Path:
    """插件目录 → 项目根。"""
    return Path(__file__).resolve().parents[2]


def _db_path() -> Path:
    return _project_root() / "data" / "plugins" / "chartyr.ironclaw-bridge" / "tasks.db"


def _esc_sq(text: str) -> str:
    """转义单引号，用于 SSH 命令中的 single-quoted 字符串。"""
    return text.replace("'", "'\"'\"'")


# ── DB ───────────────────────────────────────────────────────────────────


class TaskDB:
    """SQLite 任务状态管理。"""

    def __init__(self, db_path: Path) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._db_path = db_path
        self._lock = asyncio.Lock()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self._db_path), timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def init(self) -> None:
        with self._conn() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    task_id          TEXT PRIMARY KEY,
                    title            TEXT NOT NULL,
                    task             TEXT NOT NULL,
                    background       TEXT DEFAULT '',
                    group_id         TEXT DEFAULT '',
                    stream_id        TEXT DEFAULT '',
                    trigger_msg_id   TEXT DEFAULT '',
                    snapshot         TEXT DEFAULT '{}',
                    status           TEXT DEFAULT 'queued',
                    result           TEXT DEFAULT '',
                    remote_pid       TEXT DEFAULT '',
                    created_at       INTEGER,
                    completed_at     INTEGER,
                    injected         INTEGER DEFAULT 0
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_tasks_group ON tasks(group_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_tasks_injected ON tasks(injected)"
            )

    def create_task(self, task_id: str, **kw: Any) -> None:
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO tasks
                    (task_id, title, task, background, group_id, stream_id,
                     trigger_msg_id, snapshot, status, created_at, injected)
                VALUES
                    (:task_id, :title, :task, :background, :group_id, :stream_id,
                     :trigger_msg_id, :snapshot, 'running', :created_at, 0)
                """,
                {
                    "task_id": task_id,
                    "title": kw.get("title", ""),
                    "task": kw.get("task", ""),
                    "background": kw.get("background", ""),
                    "group_id": kw.get("group_id", ""),
                    "stream_id": kw.get("stream_id", ""),
                    "trigger_msg_id": kw.get("trigger_msg_id", ""),
                    "snapshot": kw.get("snapshot", "{}"),
                    "created_at": kw.get("created_at", int(time.time())),
                },
            )

    def update_task(self, task_id: str, **fields: Any) -> None:
        if not fields:
            return
        keys = ", ".join(f"{k} = :{k}" for k in fields)
        fields["task_id"] = task_id
        with self._conn() as conn:
            conn.execute(f"UPDATE tasks SET {keys} WHERE task_id = :task_id", fields)

    def get_task(self, task_id: str) -> dict[str, Any] | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
            ).fetchone()
            return dict(row) if row else None

    def get_active_tasks(self) -> list[dict[str, Any]]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM tasks WHERE status = 'running' ORDER BY created_at"
            ).fetchall()
            return [dict(r) for r in rows]

    def get_pending_for_group(self, group_id: str) -> list[dict[str, Any]]:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT * FROM tasks
                WHERE group_id = ? AND injected = 0
                  AND status IN ('completed', 'failed', 'timeout')
                ORDER BY completed_at
                """,
                (group_id,),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_recent_for_group(self, group_id: str, limit: int = 5) -> list[dict[str, Any]]:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT task_id, title, status, created_at, completed_at
                FROM tasks
                WHERE group_id = ?
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (group_id, limit),
            ).fetchall()
            return [dict(r) for r in rows]

    def mark_injected(self, task_ids: list[str]) -> None:
        if not task_ids:
            return
        placeholders = ", ".join("?" for _ in task_ids)
        with self._conn() as conn:
            conn.execute(
                f"UPDATE tasks SET injected = 1 WHERE task_id IN ({placeholders})",
                task_ids,
            )


# ── SSH executor ─────────────────────────────────────────────────────────


class SSHExecutor:
    """在 basechar 上通过 SSH 向 baremetal3 提交和轮询任务。"""

    @staticmethod
    def _ssh_prefix(cfg: IronClawSectionConfig) -> list[str]:
        cmd = [
            "ssh",
            "-o", "BatchMode=yes",
            "-o", "ConnectTimeout=10",
            "-o", "StrictHostKeyChecking=accept-new",
            "-p", str(cfg.ssh_port),
        ]
        if cfg.ssh_key:
            cmd.extend(["-i", cfg.ssh_key])
        cmd.append(f"{cfg.ssh_user}@{cfg.ssh_host}")
        return cmd

    @staticmethod
    async def submit(cfg: IronClawSectionConfig, task_id: str, message: str) -> str:
        """提交任务到远端：nohup ironclaw run → 结果文件。返回 remote hint。"""
        results_dir = cfg.remote_results_dir
        result_file = f"{results_dir}/{task_id}.json"
        # 写任务 message 到临时文件，避免命令行长度和转义问题
        msg_file = f"/tmp/ic_msg_{task_id}.txt"
        # 转义 message 内容写入远端文件
        escaped = _esc_sq(message)
        # 一条 SSH：创建目录 + 写 message + nohup 启动
        remote_script = (
            f"mkdir -p {results_dir} && "
            f"printf '%s' '{escaped}' > {msg_file} && "
            f"nohup sh -c '"
            f"/usr/local/bin/ironclaw run --no-onboard --cli-only --message \"$(cat {msg_file})\" "
            f"> {result_file} 2>&1; "
            f"echo \"EXIT=$?\" >> {result_file}"
            f"' > /dev/null 2>&1 & "
            f"echo $!"
        )
        ssh_parts = SSHExecutor._ssh_prefix(cfg) + [remote_script]
        proc = await asyncio.create_subprocess_exec(
            *ssh_parts,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await proc.communicate()
        return stdout.decode("utf-8", errors="replace").strip()

    @staticmethod
    async def poll_result(cfg: IronClawSectionConfig, task_id: str) -> str | None:
        """短 SSH 调用，检查结果文件是否存在。存在则返回内容。"""
        result_file = f"{cfg.remote_results_dir}/{task_id}.json"
        cmd = SSHExecutor._ssh_prefix(cfg) + [f"cat {result_file} 2>/dev/null || echo __NOFILE__"]
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await proc.communicate()
        raw = stdout.decode("utf-8", errors="replace").strip()
        if not raw or raw == "__NOFILE__":
            return None
        return raw

    @staticmethod
    async def cancel_remote(cfg: IronClawSectionConfig, task_id: str) -> None:
        """远端清理：删除结果文件和消息文件。"""
        results_dir = cfg.remote_results_dir
        cmd = SSHExecutor._ssh_prefix(cfg) + [
            f"rm -f {results_dir}/{task_id}.json /tmp/ic_msg_{task_id}.txt 2>/dev/null; echo done"
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        await proc.communicate()


# ── plugin ───────────────────────────────────────────────────────────────


class IronClawBridgePlugin(MaiBotPlugin):
    """Planner 远程 Agent 委托插件。"""

    config_model = IronClawConfig

    def __init__(self) -> None:
        super().__init__()
        self._db: TaskDB | None = None
        self._poller_started = False

    async def on_load(self) -> None:
        self._db = TaskDB(_db_path())
        self._db.init()
        logger.info("[IronClawBridge] 插件已加载，DB 初始化完成")
        if not self._poller_started:
            self._poller_started = True
            asyncio.create_task(self._poll_loop())
            logger.info("[IronClawBridge] 后台轮询已启动")

    async def on_unload(self) -> None:
        logger.info("[IronClawBridge] 插件已卸载")
        self._poller_started = False

    async def on_config_update(self, scope: str, config_data: dict[str, Any], version: str) -> None:
        del scope, config_data, version
        logger.info("[IronClawBridge] 配置已更新")

    def _cfg(self) -> IronClawSectionConfig:
        try:
            return self.config.ironclaw
        except Exception:
            return IronClawSectionConfig()

    def _extract_stream_info(self, kwargs: dict[str, Any]) -> tuple[str, str]:
        """从 Tool 调用 kwargs 中提取 stream_id 和 group_id。"""
        stream_id = str(kwargs.get("stream_id") or kwargs.get("chat_id") or kwargs.get("session_id") or "").strip()
        group_id = ""
        if not stream_id:
            message = kwargs.get("message")
            if isinstance(message, dict):
                info = message.get("message_info") or {}
                stream_id = str(
                    message.get("session_id")
                    or info.get("stream_id")
                    or info.get("chat_id")
                    or ""
                ).strip()
                gi = info.get("group_info") or {}
                group_id = str(gi.get("group_id") or "").strip()
        if not group_id:
            group_id = stream_id
        return stream_id, group_id

    async def _get_chat_snapshot(self, stream_id: str, count: int) -> list[dict[str, str]]:
        """通过 message.get_recent 抓取最近群聊消息作为快照。"""
        if not stream_id or count <= 0:
            return []
        try:
            raw = await self.ctx.call_capability(
                "message.get_recent",
                stream_id=stream_id,
                limit=count,
            )
            if not isinstance(raw, dict):
                return []
            messages = raw.get("messages") or raw.get("data") or []
            if not isinstance(messages, list):
                return []
            snapshot: list[dict[str, str]] = []
            for msg in messages:
                if not isinstance(msg, dict):
                    continue
                text = str(
                    msg.get("processed_plain_text")
                    or msg.get("plain_text")
                    or msg.get("text")
                    or ""
                ).strip()
                sender = str(
                    msg.get("user_nickname")
                    or msg.get("nickname")
                    or msg.get("sender_name")
                    or ""
                ).strip()
                ts = msg.get("time")
                if isinstance(ts, (int, float)):
                    ts_str = datetime.fromtimestamp(ts).strftime("%H:%M")
                else:
                    ts_str = str(ts or "")[:5]
                if text:
                    snapshot.append({"sender": sender, "text": text[:200], "time": ts_str})
            return snapshot[-count:]
        except Exception as exc:
            logger.warning(f"[IronClawBridge] 快照抓取失败: {exc}")
            return []

    # ── Tool: delegate_to_agent ───────────────────────────────────────

    @Tool(
        "delegate_to_agent",
        description=(
            "将任务委托给远程自主 Agent（IronClaw）异步执行。"
            "该 Agent 具备代码编写与运行、联网搜索、文件读写、多步推理能力。"
            "适用于需要持续工作的复杂任务：写代码、分析日志、深度调研、文件处理等。"
            "\n\n"
            "已有工具（web_search、query_memory、draw_picture 等）能做的事优先用它们，"
            "本工具覆盖的是需要写代码或长时间多步推理的场景。"
            "\n\n"
            "提交后立即返回 task_id，Agent 在后台执行。"
            "完成后结果会自动反馈给你，无需轮询。"
            "如果任务需要你提供更多信息，也会回来问你。"
        ),
        parameters=[
            _tool_param(
                "title",
                ToolParamType.STRING,
                "简短任务标题（10-20字），用于后续识别和复用。例如：分析STS2补丁性能",
                required=True,
            ),
            _tool_param(
                "task",
                ToolParamType.STRING,
                "详细任务描述。要做什么、产出什么。不要直接粘贴群聊原文。",
                required=True,
            ),
            _tool_param(
                "background",
                ToolParamType.STRING,
                "可选背景信息。相关上下文、约束、已有结论等。",
                required=False,
            ),
        ],
    )
    async def handle_delegate_to_agent(
        self,
        title: str = "",
        task: str = "",
        background: str = "",
        **kwargs: Any,
    ) -> tuple[bool, str, int]:
        title = str(title or "").strip()
        task = str(task or "").strip()
        if not task:
            return False, "缺少 task：请描述要委托的任务", 0

        cfg = self._cfg()
        stream_id, group_id = self._extract_stream_info(kwargs)

        # 并发检查
        active = self._db.get_active_tasks() if self._db else []
        if len(active) >= cfg.max_concurrent:
            return (
                False,
                f"已有 {len(active)} 个任务在运行（上限 {cfg.max_concurrent}），请等其中一个完成后再提交",
                0,
            )

        task_id = f"ic_{int(time.time())}_{uuid.uuid4().hex[:8]}"

        # 抓快照
        snapshot_data = {
            "title": title,
            "task": task,
            "background": background,
            "trigger_msg_id": str(kwargs.get("msg_id") or kwargs.get("message_id") or "").strip(),
            "dispatch_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "stream_id": stream_id,
            "recent_chat": await self._get_chat_snapshot(stream_id, cfg.snapshot_chat_count),
        }

        if self._db:
            self._db.create_task(
                task_id,
                title=title,
                task=task,
                background=background,
                group_id=group_id,
                stream_id=stream_id,
                trigger_msg_id=snapshot_data["trigger_msg_id"],
                snapshot=json.dumps(snapshot_data, ensure_ascii=False),
            )

        # 构造 IronClaw message
        ic_message = task
        if background:
            ic_message = f"{task}\n\n背景信息：\n{background}"

        # SSH 提交（async）
        try:
            await SSHExecutor.submit(cfg, task_id, ic_message)
            logger.info(f"[IronClawBridge] 任务已提交: task_id={task_id} title={title}")
            return True, f"任务已提交（task_id={task_id}）。Agent 正在后台执行，完成后会自动反馈。", 1
        except Exception as exc:
            logger.error(f"[IronClawBridge] SSH 提交失败: {exc}", exc_info=True)
            if self._db:
                self._db.update_task(task_id, status="failed", result=f"SSH 提交失败: {exc}")
            return False, f"任务提交失败：{exc}", 0

    # ── Tool: ironclaw_status ─────────────────────────────────────────

    @Tool(
        "ironclaw_status",
        description=(
            "查询 IronClaw 远程 Agent 任务状态和结果。"
            "用于查看已提交任务是否完成，或回顾历史任务结果。"
        ),
        parameters=[
            _tool_param(
                "task_id",
                ToolParamType.STRING,
                "要查询的任务 ID。留空则列出当前所有活跃任务。",
                required=False,
            ),
        ],
    )
    async def handle_ironclaw_status(
        self,
        task_id: str = "",
        **kwargs: Any,
    ) -> tuple[bool, str, int]:
        if not self._db:
            return False, "数据库未初始化", 0

        task_id = str(task_id or "").strip()
        if task_id:
            task = self._db.get_task(task_id)
            if not task:
                return False, f"未找到任务 {task_id}", 0
            status = task["status"]
            result = task.get("result") or ""
            if result and len(result) > self._cfg().max_result_chars:
                result = result[: self._cfg().max_result_chars] + "…"
            title = task.get("title") or ""
            return True, f"任务「{title}」状态：{status}\n结果：{result or '（无）'}", 1

        # 列出活跃任务
        active = self._db.get_active_tasks()
        if not active:
            stream_id, group_id = self._extract_stream_info(kwargs)
            recent = self._db.get_recent_for_group(group_id, limit=5) if group_id else []
            if not recent:
                return True, "当前没有活跃任务，也没有近期任务记录。", 1
            lines = ["近期任务："]
            for t in recent:
                ts = datetime.fromtimestamp(t.get("created_at") or 0).strftime("%H:%M")
                lines.append(f"  [{t['status']}] {t.get('title', '')} ({ts})")
            return True, "\n".join(lines), 1

        lines = [f"当前有 {len(active)} 个活跃任务："]
        for t in active:
            elapsed = int(time.time()) - (t.get("created_at") or int(time.time()))
            lines.append(f"  - {t['task_id']}: {t.get('title', '')}（{elapsed}s）")
        return True, "\n".join(lines), 1

    # ── Tool: ironclaw_cancel ─────────────────────────────────────────

    @Tool(
        "ironclaw_cancel",
        description="取消一个正在运行的 IronClaw 远程 Agent 任务。",
        parameters=[
            _tool_param(
                "task_id",
                ToolParamType.STRING,
                "要取消的任务 ID。",
                required=True,
            ),
        ],
    )
    async def handle_ironclaw_cancel(
        self,
        task_id: str = "",
        **kwargs: Any,
    ) -> tuple[bool, str, int]:
        if not self._db:
            return False, "数据库未初始化", 0
        task_id = str(task_id or "").strip()
        if not task_id:
            return False, "缺少 task_id", 0
        task = self._db.get_task(task_id)
        if not task:
            return False, f"未找到任务 {task_id}", 0
        if task["status"] != "running":
            return False, f"任务 {task_id} 状态为 {task['status']}，无需取消", 0
        cfg = self._cfg()
        try:
            await SSHExecutor.cancel_remote(cfg, task_id)
        except Exception:
            pass
        self._db.update_task(task_id, status="cancelled", completed_at=int(time.time()))
        return True, f"任务 {task_id} 已取消", 1

    # ── 后台轮询 ──────────────────────────────────────────────────────

    async def _poll_loop(self) -> None:
        """后台循环：每 poll_interval 秒检查活跃任务。"""
        while self._poller_started:
            try:
                await asyncio.sleep(self._cfg().poll_interval)
                if not self._db:
                    continue
                active = self._db.get_active_tasks()
                for task in active:
                    await self._check_one(task)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning(f"[IronClawBridge] 轮询异常: {exc}")

    async def _check_one(self, task: dict[str, Any]) -> None:
        """检查单个任务的结果文件。"""
        task_id = task["task_id"]
        cfg = self._cfg()
        created_at = task.get("created_at") or int(time.time())

        # 超时检查
        if int(time.time()) - created_at > cfg.task_timeout:
            self._db.update_task(
                task_id,
                status="timeout",
                result="任务超时",
                completed_at=int(time.time()),
            )
            logger.info(f"[IronClawBridge] 任务超时: {task_id}")
            await self._try_trigger(task_id)
            return

        try:
            raw = await SSHExecutor.poll_result(cfg, task_id)
        except Exception as exc:
            logger.warning(f"[IronClawBridge] 轮询 {task_id} 失败: {exc}")
            return

        if raw is None:
            return  # 还没出结果

        # 解析结果
        result_text = raw.strip()
        exit_code = ""
        if "EXIT=" in result_text:
            idx = result_text.rfind("EXIT=")
            exit_line = result_text[idx:]
            result_text = result_text[:idx].strip()
            exit_code = exit_line.replace("EXIT=", "").strip()

        if not result_text:
            result_text = f"(Agent 返回空输出，exit={exit_code})"

        # 截断
        if len(result_text) > cfg.max_result_chars:
            result_text = result_text[: cfg.max_result_chars] + "…"

        status = "completed" if exit_code == "0" else "failed"
        self._db.update_task(
            task_id,
            status=status,
            result=result_text,
            completed_at=int(time.time()),
        )
        logger.info(f"[IronClawBridge] 任务完成: {task_id} status={status}")
        await self._try_trigger(task_id)

    async def _try_trigger(self, task_id: str) -> None:
        """任务完成后，用 maisaka.proactive.trigger 唤醒 Planner。"""
        if not self._db:
            return
        task = self._db.get_task(task_id)
        if not task:
            return
        stream_id = task.get("stream_id") or ""
        if not stream_id:
            logger.warning(f"[IronClawBridge] 任务 {task_id} 无 stream_id，无法触发")
            return

        # 构造注入文本
        snapshot = json.loads(task.get("snapshot") or "{}")
        injection = self._format_injection(task, snapshot)

        # 先用 maisaka.context.append 把结果插入 chat_history
        try:
            await self.ctx.call_capability(
                "maisaka.context.append",
                stream_id=stream_id,
                segments=[
                    {
                        "type": "text",
                        "text": injection,
                    }
                ],
                visible_text=injection,
                source_kind="plugin:chartyr.ironclaw-bridge",
            )
            logger.info(f"[IronClawBridge] 结果已注入 chat_history: {task_id}")
        except Exception as exc:
            logger.error(f"[IronClawBridge] context.append 失败: {exc}", exc_info=True)

        # 再用 maisaka.proactive.trigger 唤醒 Planner
        intent = f"之前委派给远程 Agent 的任务「{task.get('title', '')}」已完成，请查看上面的结果并决定如何回复。"
        try:
            await self.ctx.call_capability(
                "maisaka.proactive.trigger",
                stream_id=stream_id,
                intent=intent,
                reason=f"ironclaw task {task_id} completed",
            )
            logger.info(f"[IronClawBridge] Planner 已唤醒: {task_id}")
        except Exception as exc:
            logger.error(f"[IronClawBridge] proactive.trigger 失败: {exc}", exc_info=True)

    def _format_injection(self, task: dict[str, Any], snapshot: dict[str, Any]) -> str:
        """构造注入 Planner 上下文的文本。"""
        lines = ["【远端 Agent 任务反馈】"]

        # 任务回顾
        lines.append("═══ 任务回顾 ═══")
        dispatch_time = snapshot.get("dispatch_time") or ""
        lines.append(f"委派时间：{dispatch_time}")
        lines.append(f"任务标题：{task.get('title', '')}")
        lines.append(f"任务内容：{task.get('task', '')}")
        if task.get("background"):
            lines.append(f"背景：{task['background']}")

        # 当时的群聊快照
        recent_chat = snapshot.get("recent_chat") or []
        if recent_chat:
            lines.append("当时群聊片段：")
            for msg in recent_chat[-5:]:
                sender = msg.get("sender") or "?"
                text = msg.get("text") or ""
                ts = msg.get("time") or ""
                lines.append(f"  [{ts}] {sender}: {text}")

        # 结果
        lines.append("")
        lines.append("═══ 执行结果 ═══")
        lines.append(f"状态：{task.get('status', 'unknown')}")
        result = task.get("result") or "（无结果）"
        lines.append(f"结果：{result}")

        # 同群其他任务摘要
        group_id = task.get("group_id") or ""
        if group_id and self._db:
            recent = self._db.get_recent_for_group(group_id, limit=5)
            others = [r for r in recent if r["task_id"] != task["task_id"]]
            if others:
                lines.append("")
                lines.append("═══ 同群其他任务 ═══")
                for r in others:
                    ts = datetime.fromtimestamp(r.get("created_at") or 0).strftime("%H:%M")
                    lines.append(f"  [{r['status']}] {r.get('title', '')} ({ts})")

        return "\n".join(lines)


def create_plugin() -> IronClawBridgePlugin:
    return IronClawBridgePlugin()
