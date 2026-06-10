"""Git 记忆仓库工具。

【中文名称】Git 记忆仓库工具

【功能说明】
负责用 dulwich 给记忆文件建立轻量 Git 历史，支持自动提交、diff、blame 和时间线展示。

【在整体架构中的位置】
该文件属于 P1 范围的通用工具代码：它不改变 Agent 主循环的骨架，
而是负责把某一种外部协议、模型接口或通用能力接到 nanobot 的统一抽象上。

【学习重点】
- 先看本文件的配置类/数据类，理解外部服务需要哪些参数。
- 再看 start/stop/send 或 generate/stream 等入口方法，理解数据如何进出。
- 最后看私有辅助函数，它们通常是在处理平台限制、协议兼容或安全边界。"""

from __future__ import annotations

import io
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from loguru import logger


@dataclass
class CommitInfo:
    """CommitInfo 类。

    【中文名称】CommitInfo

    【功能说明】
    这是 Git 记忆仓库工具 中的核心数据结构或服务类。负责用 dulwich 给记忆文件建立轻量 Git 历史，支持自动提交、diff、blame 和时间线展示。

    【学习重点】
    - 类属性/字段通常描述外部平台、模型或工具的配置。
    - public 方法通常是其他模块会调用的入口。
    - private 方法通常负责协议细节、格式转换或异常兜底。"""

    sha: str  # 说明：这里处理 Git 记忆仓库工具 的协议细节或边界情况，避免外部差异影响核心流程。
    message: str
    timestamp: str  # 说明：这里处理 Git 记忆仓库工具 的协议细节或边界情况，避免外部差异影响核心流程。

    def format(self, diff: str = "") -> str:
        """执行 `format`。

        【中文名称】format

        【功能说明】
        这是 Git 记忆仓库工具 中的一个步骤函数，用来支撑：负责用 dulwich 给记忆文件建立轻量 Git 历史，支持自动提交、diff、blame 和时间线展示。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - diff: 调用方传入的 `diff` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        header = f"## {self.message.splitlines()[0]}\n`{self.sha}` — {self.timestamp}\n"
        if diff:
            return f"{header}\n```diff\n{diff}\n```"
        return f"{header}\n(no file changes)"


@dataclass
class LineAge:
    """LineAge 类。

    【中文名称】LineAge

    【功能说明】
    这是 Git 记忆仓库工具 中的核心数据结构或服务类。负责用 dulwich 给记忆文件建立轻量 Git 历史，支持自动提交、diff、blame 和时间线展示。

    【学习重点】
    - 类属性/字段通常描述外部平台、模型或工具的配置。
    - public 方法通常是其他模块会调用的入口。
    - private 方法通常负责协议细节、格式转换或异常兜底。"""

    age_days: int  # 说明：这里处理 Git 记忆仓库工具 的协议细节或边界情况，避免外部差异影响核心流程。


def _compute_line_ages(annotated) -> list[LineAge]:
    """执行 `_compute_line_ages`。

    【中文名称】_compute_line_ages

    【功能说明】
    这是 Git 记忆仓库工具 中的一个步骤函数，用来支撑：负责用 dulwich 给记忆文件建立轻量 Git 历史，支持自动提交、diff、blame 和时间线展示。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - annotated: 调用方传入的 `annotated` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    now = datetime.now(tz=timezone.utc).date()
    ages: list[LineAge] = []
    for (commit, _tree_entry), _line_bytes in annotated:
        dt = datetime.fromtimestamp(commit.commit_time, tz=timezone.utc).date()
        ages.append(LineAge(age_days=(now - dt).days))
    return ages


class GitStore:
    """GitStore 类。

    【中文名称】GitStore

    【功能说明】
    这是 Git 记忆仓库工具 中的核心数据结构或服务类。负责用 dulwich 给记忆文件建立轻量 Git 历史，支持自动提交、diff、blame 和时间线展示。

    【学习重点】
    - 类属性/字段通常描述外部平台、模型或工具的配置。
    - public 方法通常是其他模块会调用的入口。
    - private 方法通常负责协议细节、格式转换或异常兜底。"""

    def __init__(self, workspace: Path, tracked_files: list[str]):
        """执行 `__init__`。

        【中文名称】__init__

        【功能说明】
        这是 Git 记忆仓库工具 中的一个步骤函数，用来支撑：负责用 dulwich 给记忆文件建立轻量 Git 历史，支持自动提交、diff、blame 和时间线展示。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - workspace: 调用方传入的 `workspace` 数据；具体类型以函数签名为准。
        - tracked_files: 调用方传入的 `tracked_files` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

        self._workspace = workspace
        self._tracked_files = tracked_files

    def is_initialized(self) -> bool:
        """执行 `is_initialized`。

        【中文名称】is_initialized

        【功能说明】
        这是 Git 记忆仓库工具 中的一个步骤函数，用来支撑：负责用 dulwich 给记忆文件建立轻量 Git 历史，支持自动提交、diff、blame 和时间线展示。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        return (self._workspace / ".git").is_dir()

    # 说明：这里处理 Git 记忆仓库工具 的协议细节或边界情况，避免外部差异影响核心流程。

    def init(self) -> bool:
        """执行 `init`。

        【中文名称】init

        【功能说明】
        这是 Git 记忆仓库工具 中的一个步骤函数，用来支撑：负责用 dulwich 给记忆文件建立轻量 Git 历史，支持自动提交、diff、blame 和时间线展示。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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

            # 说明：这里处理 Git 记忆仓库工具 的协议细节或边界情况，避免外部差异影响核心流程。
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

            # 说明：这里处理 Git 记忆仓库工具 的协议细节或边界情况，避免外部差异影响核心流程。
            # 说明：这里处理 Git 记忆仓库工具 的协议细节或边界情况，避免外部差异影响核心流程。
            for rel in self._tracked_files:
                p = self._workspace / rel
                p.parent.mkdir(parents=True, exist_ok=True)
                if not p.exists():
                    p.write_text("", encoding="utf-8")

            # 说明：这里处理 Git 记忆仓库工具 的协议细节或边界情况，避免外部差异影响核心流程。
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

    # 说明：这里处理 Git 记忆仓库工具 的协议细节或边界情况，避免外部差异影响核心流程。

    def auto_commit(self, message: str) -> str | None:
        """执行 `auto_commit`。

        【中文名称】auto_commit

        【功能说明】
        这是 Git 记忆仓库工具 中的一个步骤函数，用来支撑：负责用 dulwich 给记忆文件建立轻量 Git 历史，支持自动提交、diff、blame 和时间线展示。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - message: 调用方传入的 `message` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        if not self.is_initialized():
            return None

        try:
            from dulwich import porcelain

            # 说明：这里处理 Git 记忆仓库工具 的协议细节或边界情况，避免外部差异影响核心流程。
            # 说明：这里处理 Git 记忆仓库工具 的协议细节或边界情况，避免外部差异影响核心流程。
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

    # 说明：这里处理 Git 记忆仓库工具 的协议细节或边界情况，避免外部差异影响核心流程。

    def _resolve_sha(self, short_sha: str) -> bytes | None:
        """执行 `_resolve_sha`。

        【中文名称】_resolve_sha

        【功能说明】
        这是 Git 记忆仓库工具 中的一个步骤函数，用来支撑：负责用 dulwich 给记忆文件建立轻量 Git 历史，支持自动提交、diff、blame 和时间线展示。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - short_sha: 调用方传入的 `short_sha` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
        """执行 `_is_inside_git_repo`。

        【中文名称】_is_inside_git_repo

        【功能说明】
        这是 Git 记忆仓库工具 中的一个步骤函数，用来支撑：负责用 dulwich 给记忆文件建立轻量 Git 历史，支持自动提交、diff、blame 和时间线展示。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        current = self._workspace.resolve()
        while current != current.parent:
            if (current / ".git").exists():
                return True
            current = current.parent
        return False

    def _build_gitignore(self) -> str:
        """执行 `_build_gitignore`。

        【中文名称】_build_gitignore

        【功能说明】
        这是 Git 记忆仓库工具 中的一个步骤函数，用来支撑：负责用 dulwich 给记忆文件建立轻量 Git 历史，支持自动提交、diff、blame 和时间线展示。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - 无显式业务参数。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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

    # 说明：这里处理 Git 记忆仓库工具 的协议细节或边界情况，避免外部差异影响核心流程。

    def log(self, max_entries: int = 20) -> list[CommitInfo]:
        """执行 `log`。

        【中文名称】log

        【功能说明】
        这是 Git 记忆仓库工具 中的一个步骤函数，用来支撑：负责用 dulwich 给记忆文件建立轻量 Git 历史，支持自动提交、diff、blame 和时间线展示。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - max_entries: 调用方传入的 `max_entries` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
        """执行 `line_ages`。

        【中文名称】line_ages

        【功能说明】
        这是 Git 记忆仓库工具 中的一个步骤函数，用来支撑：负责用 dulwich 给记忆文件建立轻量 Git 历史，支持自动提交、diff、blame 和时间线展示。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - file_path: 调用方传入的 `file_path` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""

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
        """执行 `diff_commits`。

        【中文名称】diff_commits

        【功能说明】
        这是 Git 记忆仓库工具 中的一个步骤函数，用来支撑：负责用 dulwich 给记忆文件建立轻量 Git 历史，支持自动提交、diff、blame 和时间线展示。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - sha1: 调用方传入的 `sha1` 数据；具体类型以函数签名为准。
        - sha2: 调用方传入的 `sha2` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
        """执行 `find_commit`。

        【中文名称】find_commit

        【功能说明】
        这是 Git 记忆仓库工具 中的一个步骤函数，用来支撑：负责用 dulwich 给记忆文件建立轻量 Git 历史，支持自动提交、diff、blame 和时间线展示。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - short_sha: 调用方传入的 `short_sha` 数据；具体类型以函数签名为准。
        - max_entries: 调用方传入的 `max_entries` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        for c in self.log(max_entries=max_entries):
            if c.sha.startswith(short_sha):
                return c
        return None

    def show_commit_diff(self, short_sha: str, max_entries: int = 20) -> tuple[CommitInfo, str] | None:
        """执行 `show_commit_diff`。

        【中文名称】show_commit_diff

        【功能说明】
        这是 Git 记忆仓库工具 中的一个步骤函数，用来支撑：负责用 dulwich 给记忆文件建立轻量 Git 历史，支持自动提交、diff、blame 和时间线展示。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - short_sha: 调用方传入的 `short_sha` 数据；具体类型以函数签名为准。
        - max_entries: 调用方传入的 `max_entries` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
        commits = self.log(max_entries=max_entries)
        for i, c in enumerate(commits):
            if c.sha.startswith(short_sha):
                if i + 1 < len(commits):
                    diff = self.diff_commits(commits[i + 1].sha, c.sha)
                else:
                    diff = ""
                return c, diff
        return None

    # 说明：这里处理 Git 记忆仓库工具 的协议细节或边界情况，避免外部差异影响核心流程。

    def revert(self, commit: str) -> str | None:
        """执行 `revert`。

        【中文名称】revert

        【功能说明】
        这是 Git 记忆仓库工具 中的一个步骤函数，用来支撑：负责用 dulwich 给记忆文件建立轻量 Git 历史，支持自动提交、diff、blame 和时间线展示。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - commit: 调用方传入的 `commit` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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

                # 说明：这里处理 Git 记忆仓库工具 的协议细节或边界情况，避免外部差异影响核心流程。
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

            # 说明：这里处理 Git 记忆仓库工具 的协议细节或边界情况，避免外部差异影响核心流程。
            msg = f"revert: undo {commit}"
            return self.auto_commit(msg)
        except Exception:
            logger.exception("Git revert failed for {}", commit)
            return None

    @staticmethod
    def _read_blob_from_tree(repo, tree, filepath: str) -> str | None:
        """执行 `_read_blob_from_tree`。

        【中文名称】_read_blob_from_tree

        【功能说明】
        这是 Git 记忆仓库工具 中的一个步骤函数，用来支撑：负责用 dulwich 给记忆文件建立轻量 Git 历史，支持自动提交、diff、blame 和时间线展示。
        阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

        【参数说明】
        - repo: 调用方传入的 `repo` 数据；具体类型以函数签名为准。
        - tree: 调用方传入的 `tree` 数据；具体类型以函数签名为准。
        - filepath: 调用方传入的 `filepath` 数据；具体类型以函数签名为准。

        【返回值】
        - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
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
