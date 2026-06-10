"""用 Git 仓库保存轻量键值数据，方便配置和历史记录持久化。

【中文名称】工具模块：nanobot/utils/gitstore.py

【功能说明】
本文件属于 P1 学习范围，重点帮助初学者理解“外部系统 ↔ nanobot 后端”之间的适配层。
阅读时可以先看类和函数的中文说明，再沿着消息、配置、异常和返回值四条线索跟代码。

【主要职责】
1. 接收配置或输入数据，整理成后端内部统一使用的结构。
2. 调用第三方 SDK、HTTP API 或公共工具函数完成实际工作。
3. 把外部返回值、错误和流式事件转换成 nanobot 可继续处理的数据。
4. 在边界处处理鉴权、限流、媒体文件、重试和日志，避免复杂度泄漏到核心 Agent。

【学习提示】
如果你是 Agent 或后端初学者，可以把本文件看成“翻译器”：它不改变核心 Agent 思路，
而是负责理解某个平台或服务商的协议，并把它翻译成项目内部约定的数据形状。
"""

from __future__ import annotations

import io
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from loguru import logger


@dataclass
class CommitInfo:
    """CommitInfo 类，封装 工具模块 的核心状态和行为。

    【中文名称】CommitInfo

    【功能说明】
    用 Git 仓库保存轻量键值数据，方便配置和历史记录持久化。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    普通 Python 类。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """
    sha: str  # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
    message: str
    timestamp: str  # 中文说明：这一段围绕格式处理，注意输入、输出和异常路径。

    def format(self, diff: str = "") -> str:
        """格式化内容（format = 原函数名）。

        【中文名称】格式化内容

        【功能说明】
        这是 工具模块 中的一个关键步骤。用 Git 仓库保存轻量键值数据，方便配置和历史记录持久化。
        在阅读 `CommitInfo.format` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        diff: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        header = f"## {self.message.splitlines()[0]}\n`{self.sha}` — {self.timestamp}\n"
        if diff:
            return f"{header}\n```diff\n{diff}\n```"
        return f"{header}\n(no file changes)"


@dataclass
class LineAge:
    """LineAge 类，封装 工具模块 的核心状态和行为。

    【中文名称】LineAge

    【功能说明】
    用 Git 仓库保存轻量键值数据，方便配置和历史记录持久化。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    普通 Python 类。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """

    age_days: int  # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。


def _compute_line_ages(annotated) -> list[LineAge]:
    """执行辅助逻辑（_compute_line_ages = 原函数名）。

    【中文名称】执行辅助逻辑

    【功能说明】
    这是 工具模块 中的一个关键步骤。用 Git 仓库保存轻量键值数据，方便配置和历史记录持久化。
    在阅读 `_compute_line_ages` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

    【参数说明】
    annotated: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

    【返回值】
    返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
    """
    now = datetime.now(tz=timezone.utc).date()
    ages: list[LineAge] = []
    for (commit, _tree_entry), _line_bytes in annotated:
        dt = datetime.fromtimestamp(commit.commit_time, tz=timezone.utc).date()
        ages.append(LineAge(age_days=(now - dt).days))
    return ages


class GitStore:
    """GitStore 类，封装 工具模块 的核心状态和行为。

    【中文名称】GitStore

    【功能说明】
    用 Git 仓库保存轻量键值数据，方便配置和历史记录持久化。 这个类把相关配置、客户端连接和消息处理方法放在一起，
    让外层代码只需要通过统一接口调用，而不用关心平台或服务商的协议细节。

    【继承关系】
    普通 Python 类。继承关系决定它需要实现哪些项目约定的方法。

    【学习提示】
    先看 __init__ 如何保存配置，再看 start/stop 或 send/handle 类方法如何连接外部世界。
    """

    def __init__(self, workspace: Path, tracked_files: list[str]):
        """初始化对象（__init__ = 原函数名）。

        【中文名称】初始化对象

        【功能说明】
        这是 工具模块 中的一个关键步骤。用 Git 仓库保存轻量键值数据，方便配置和历史记录持久化。
        在阅读 `GitStore.__init__` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        workspace: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        tracked_files: 文件或路径信息，代码会按安全边界读取或写入。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        self._workspace = workspace
        self._tracked_files = tracked_files

    def is_initialized(self) -> bool:
        """判断条件是否成立（is_initialized = 原函数名）。

        【中文名称】判断条件是否成立

        【功能说明】
        这是 工具模块 中的一个关键步骤。用 Git 仓库保存轻量键值数据，方便配置和历史记录持久化。
        在阅读 `GitStore.is_initialized` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        return (self._workspace / ".git").is_dir()

    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。

    def init(self) -> bool:
        """执行辅助逻辑（init = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 工具模块 中的一个关键步骤。用 Git 仓库保存轻量键值数据，方便配置和历史记录持久化。
        在阅读 `GitStore.init` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if self.is_initialized():
            return False

        if self._is_inside_git_repo():
            logger.warning(
                "Workspace {} is already inside a git repo; "
                "skipping nested repo initialization",
                self._workspace,
            )
            return False

        try:
            from dulwich import porcelain

            porcelain.init(str(self._workspace))

            # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
            gitignore = self._workspace / ".gitignore"
            dream_entries = self._build_gitignore()
            if gitignore.exists():
                existing = gitignore.read_text(encoding="utf-8")
                existing_lines = set(existing.splitlines())
                new_lines = [
                    line
                    for line in dream_entries.splitlines()
                    if line not in existing_lines
                ]
                if new_lines:
                    merged = existing.rstrip("\n") + "\n" + "\n".join(new_lines) + "\n"
                    gitignore.write_text(merged, encoding="utf-8")
            else:
                gitignore.write_text(dream_entries, encoding="utf-8")

            # 中文说明：这一段围绕文件处理，注意输入、输出和异常路径。
            # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
            for rel in self._tracked_files:
                p = self._workspace / rel
                p.parent.mkdir(parents=True, exist_ok=True)
                if not p.exists():
                    p.write_text("", encoding="utf-8")

            # 中文说明：Initial commit 相关逻辑。
            porcelain.add(str(self._workspace), paths=[".gitignore"] + self._tracked_files)
            porcelain.commit(
                str(self._workspace),
                message=b"init: nanobot memory store",
                author=b"nanobot <nanobot@dream>",
                committer=b"nanobot <nanobot@dream>",
            )
            logger.info("Git store initialized at {}", self._workspace)
            return True
        except Exception:
            logger.exception("Git store init failed for {}", self._workspace)
            return False

    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。

    def auto_commit(self, message: str) -> str | None:
        """执行辅助逻辑（auto_commit = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 工具模块 中的一个关键步骤。用 Git 仓库保存轻量键值数据，方便配置和历史记录持久化。
        在阅读 `GitStore.auto_commit` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        message: 消息数据，可能来自用户、频道、模型或工具调用。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not self.is_initialized():
            return None

        try:
            from dulwich import porcelain

            # 中文说明：这一段围绕文件处理，注意输入、输出和异常路径。
            # 中文说明：这一段围绕文件处理，注意输入、输出和异常路径。
            st = porcelain.status(str(self._workspace))
            if not st.unstaged and not any(st.staged.values()):
                return None

            msg_bytes = message.encode("utf-8") if isinstance(message, str) else message
            porcelain.add(str(self._workspace), paths=self._tracked_files)
            sha_bytes = porcelain.commit(
                str(self._workspace),
                message=msg_bytes,
                author=b"nanobot <nanobot@dream>",
                committer=b"nanobot <nanobot@dream>",
            )
            if sha_bytes is None:
                return None
            sha = sha_bytes.hex()[:8]
            logger.debug("Git auto-commit: {} ({})", sha, message)
            return sha
        except Exception:
            logger.exception("Git auto-commit failed: {}", message)
            return None

    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。

    def _resolve_sha(self, short_sha: str) -> bytes | None:
        """解析目标（_resolve_sha = 原函数名）。

        【中文名称】解析目标

        【功能说明】
        这是 工具模块 中的一个关键步骤。用 Git 仓库保存轻量键值数据，方便配置和历史记录持久化。
        在阅读 `GitStore._resolve_sha` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        short_sha: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        try:
            from dulwich.repo import Repo

            with Repo(str(self._workspace)) as repo:
                try:
                    sha = repo.refs[b"HEAD"]
                except KeyError:
                    return None

                while sha:
                    if sha.hex().startswith(short_sha):
                        return sha
                    commit = repo[sha]
                    if commit.type_name != b"commit":
                        break
                    sha = commit.parents[0] if commit.parents else None
            return None
        except Exception:
            return None

    def _is_inside_git_repo(self) -> bool:
        """判断条件是否成立（_is_inside_git_repo = 原函数名）。

        【中文名称】判断条件是否成立

        【功能说明】
        这是 工具模块 中的一个关键步骤。用 Git 仓库保存轻量键值数据，方便配置和历史记录持久化。
        在阅读 `GitStore._is_inside_git_repo` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        current = self._workspace.resolve()
        while current != current.parent:
            if (current / ".git").exists():
                return True
            current = current.parent
        return False

    def _build_gitignore(self) -> str:
        """构建对象（_build_gitignore = 原函数名）。

        【中文名称】构建对象

        【功能说明】
        这是 工具模块 中的一个关键步骤。用 Git 仓库保存轻量键值数据，方便配置和历史记录持久化。
        在阅读 `GitStore._build_gitignore` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        dirs: set[str] = set()
        for f in self._tracked_files:
            parent = str(Path(f).parent)
            if parent != ".":
                dirs.add(parent)
        lines = ["/*"]
        for d in sorted(dirs):
            lines.append(f"!{d}/")
        for f in self._tracked_files:
            lines.append(f"!{f}")
        lines.append("!.gitignore")
        return "\n".join(lines) + "\n"

    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。

    def log(self, max_entries: int = 20) -> list[CommitInfo]:
        """执行辅助逻辑（log = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 工具模块 中的一个关键步骤。用 Git 仓库保存轻量键值数据，方便配置和历史记录持久化。
        在阅读 `GitStore.log` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        max_entries: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not self.is_initialized():
            return []

        try:
            from dulwich.repo import Repo

            entries: list[CommitInfo] = []
            with Repo(str(self._workspace)) as repo:
                try:
                    head = repo.refs[b"HEAD"]
                except KeyError:
                    return []

                sha = head
                while sha and len(entries) < max_entries:
                    commit = repo[sha]
                    if commit.type_name != b"commit":
                        break
                    ts = time.strftime(
                        "%Y-%m-%d %H:%M",
                        time.localtime(commit.commit_time),
                    )
                    msg = commit.message.decode("utf-8", errors="replace").strip()
                    entries.append(CommitInfo(
                        sha=sha.hex()[:8],
                        message=msg,
                        timestamp=ts,
                    ))
                    sha = commit.parents[0] if commit.parents else None

            return entries
        except Exception:
            logger.exception("Git log failed")
            return []

    def line_ages(self, file_path: str) -> list[LineAge]:
        """执行辅助逻辑（line_ages = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 工具模块 中的一个关键步骤。用 Git 仓库保存轻量键值数据，方便配置和历史记录持久化。
        在阅读 `GitStore.line_ages` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        file_path: 文件或路径信息，代码会按安全边界读取或写入。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """

        if not self.is_initialized():
            return []

        target = self._workspace / file_path
        if not target.exists() or target.stat().st_size == 0:
            return []

        try:
            from dulwich import porcelain

            annotated = porcelain.annotate(str(self._workspace), file_path)
        except Exception:
            logger.exception("Git line_ages annotate failed for {}", file_path)
            return []

        if not annotated:
            return []

        return _compute_line_ages(annotated)

    def diff_commits(self, sha1: str, sha2: str) -> str:
        """执行辅助逻辑（diff_commits = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 工具模块 中的一个关键步骤。用 Git 仓库保存轻量键值数据，方便配置和历史记录持久化。
        在阅读 `GitStore.diff_commits` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        sha1: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        sha2: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not self.is_initialized():
            return ""

        try:
            from dulwich import porcelain

            full1 = self._resolve_sha(sha1)
            full2 = self._resolve_sha(sha2)
            if not full1 or not full2:
                return ""

            out = io.BytesIO()
            porcelain.diff(
                str(self._workspace),
                commit=full1,
                commit2=full2,
                outstream=out,
            )
            return out.getvalue().decode("utf-8", errors="replace")
        except Exception:
            logger.exception("Git diff_commits failed")
            return ""

    def find_commit(self, short_sha: str, max_entries: int = 20) -> CommitInfo | None:
        """执行辅助逻辑（find_commit = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 工具模块 中的一个关键步骤。用 Git 仓库保存轻量键值数据，方便配置和历史记录持久化。
        在阅读 `GitStore.find_commit` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        short_sha: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        max_entries: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        for c in self.log(max_entries=max_entries):
            if c.sha.startswith(short_sha):
                return c
        return None

    def show_commit_diff(self, short_sha: str, max_entries: int = 20) -> tuple[CommitInfo, str] | None:
        """执行辅助逻辑（show_commit_diff = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 工具模块 中的一个关键步骤。用 Git 仓库保存轻量键值数据，方便配置和历史记录持久化。
        在阅读 `GitStore.show_commit_diff` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        short_sha: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        max_entries: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        commits = self.log(max_entries=max_entries)
        for i, c in enumerate(commits):
            if c.sha.startswith(short_sha):
                if i + 1 < len(commits):
                    diff = self.diff_commits(commits[i + 1].sha, c.sha)
                else:
                    diff = ""
                return c, diff
        return None

    # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。

    def revert(self, commit: str) -> str | None:
        """执行辅助逻辑（revert = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 工具模块 中的一个关键步骤。用 Git 仓库保存轻量键值数据，方便配置和历史记录持久化。
        在阅读 `GitStore.revert` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        self: 当前对象或类本身，用于访问配置、客户端和共享状态。
        commit: 该函数的输入参数，具体含义可结合调用处和类型标注理解。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        if not self.is_initialized():
            return None

        try:
            from dulwich.repo import Repo

            full_sha = self._resolve_sha(commit)
            if not full_sha:
                logger.warning("Git revert: SHA not found: {}", commit)
                return None

            with Repo(str(self._workspace)) as repo:
                commit_obj = repo[full_sha]
                if commit_obj.type_name != b"commit":
                    return None

                if not commit_obj.parents:
                    logger.warning("Git revert: cannot revert root commit {}", commit)
                    return None

                # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
                parent_obj = repo[commit_obj.parents[0]]
                tree = repo[parent_obj.tree]

                restored: list[str] = []
                for filepath in self._tracked_files:
                    content = self._read_blob_from_tree(repo, tree, filepath)
                    if content is not None:
                        dest = self._workspace / filepath
                        dest.write_text(content, encoding="utf-8")
                        restored.append(filepath)

            if not restored:
                return None

            # 中文说明：这里解释当前实现细节，帮助初学者理解为什么需要这段处理。
            msg = f"revert: undo {commit}"
            return self.auto_commit(msg)
        except Exception:
            logger.exception("Git revert failed for {}", commit)
            return None

    @staticmethod
    def _read_blob_from_tree(repo, tree, filepath: str) -> str | None:
        """执行辅助逻辑（_read_blob_from_tree = 原函数名）。

        【中文名称】执行辅助逻辑

        【功能说明】
        这是 工具模块 中的一个关键步骤。用 Git 仓库保存轻量键值数据，方便配置和历史记录持久化。
        在阅读 `GitStore._read_blob_from_tree` 时，重点看它如何准备输入、调用下游能力、处理异常，并把结果整理给调用方。

        【参数说明】
        repo: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        tree: 该函数的输入参数，具体含义可结合调用处和类型标注理解。
        filepath: 文件或路径信息，代码会按安全边界读取或写入。

        【返回值】
        返回值会交给上层流程继续使用；如果函数只产生副作用，则重点关注它修改的对象状态或发送的外部请求。
        """
        parts = Path(filepath).parts
        current = tree
        for part in parts:
            try:
                entry = current[part.encode()]
            except KeyError:
                return None
            obj = repo[entry[1]]
            if obj.type_name == b"blob":
                return obj.data.decode("utf-8", errors="replace")
            if obj.type_name == b"tree":
                current = obj
            else:
                return None
        return None

