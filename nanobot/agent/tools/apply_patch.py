"""结构化补丁工具：让 Agent 以声明式编辑方式修改文件。

和直接通过 shell 跑 ``sed`` / ``echo`` / 脚本不同，这个工具把修改动作
表达为结构化数据，因此更容易做到：

- 路径合法性校验
- 精确替换
- 多文件批量修改
- 失败回滚
"""

from __future__ import annotations

import difflib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from nanobot.agent.tools.base import tool_parameters
from nanobot.agent.tools.filesystem import _FsTool
from nanobot.agent.tools.schema import (
    ArraySchema,
    BooleanSchema,
    ObjectSchema,
    StringSchema,
    tool_parameters_schema,
)


@dataclass(slots=True)
class _PatchSummary:
    """记录单个文件补丁结果的摘要。"""
    action: str
    path: str
    added: int = 0
    deleted: int = 0


class _PatchError(ValueError):
    """补丁输入非法或应用失败时使用的内部异常。"""
    pass


_ABSOLUTE_WINDOWS_RE = re.compile(r"^[A-Za-z]:[\\/]")


def _validate_relative_path(path: str) -> str:
    """校验补丁路径必须是相对路径，且不能越界。"""
    normalized = path.strip()
    if not normalized:
        raise _PatchError("patch path cannot be empty")
    if "\0" in normalized:
        raise _PatchError(f"patch path contains a null byte: {path!r}")
    if normalized.startswith(("~", "/", "\\")) or _ABSOLUTE_WINDOWS_RE.match(normalized):
        raise _PatchError(f"patch path must be relative: {path}")
    if any(part == ".." for part in re.split(r"[\\/]+", normalized)):
        raise _PatchError(f"patch path must not contain '..': {path}")
    return normalized


def _lines_to_text(lines: list[str]) -> str:
    """把行数组重新拼成带结尾换行的文本。"""
    if not lines:
        return ""
    return "\n".join(lines) + "\n"


def _text_line_count(text: str) -> int:
    """统计文本的逻辑行数。"""
    if not text:
        return 0
    return len(text.splitlines())


def _line_diff_stats(before: str, after: str) -> tuple[int, int]:
    """粗略统计修改带来的新增/删除行数。"""
    before_lines = before.replace("\r\n", "\n").splitlines()
    after_lines = after.replace("\r\n", "\n").splitlines()
    added = 0
    deleted = 0
    matcher = difflib.SequenceMatcher(a=before_lines, b=after_lines, autojunk=False)
    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == "equal":
            continue
        if tag in ("replace", "delete"):
            deleted += i2 - i1
        if tag in ("replace", "insert"):
            added += j2 - j1
    return added, deleted


def _append_text(content: str, addition: str) -> str:
    """追加文本时，避免把新内容错误拼到旧文件最后一行后面。"""
    base = content.replace("\r\n", "\n")
    extra = addition.replace("\r\n", "\n")
    if base and extra and not base.endswith("\n") and not extra.startswith("\n"):
        base += "\n"
    combined = base + extra
    if combined and not combined.endswith("\n"):
        combined += "\n"
    return combined


def _format_summary(summary: _PatchSummary) -> str:
    """把补丁摘要格式化成返回给模型的文本。"""
    stats = ""
    if summary.added or summary.deleted:
        stats = f" (+{summary.added}/-{summary.deleted})"
    return f"- {summary.action} {summary.path}{stats}"


@tool_parameters(
    tool_parameters_schema(
        edits=ArraySchema(
            items=ObjectSchema(
                path=StringSchema("Relative path to the file to edit."),
                action=StringSchema(
                    "Operation type: replace or add.",
                    enum=["replace", "add"],
                ),
                old_text=StringSchema(
                    "Exact text to search for in the file. Required for replace.",
                    nullable=True,
                ),
                new_text=StringSchema(
                    "Text to replace with or append. Required for replace and add.",
                    nullable=True,
                ),
                required=["path", "action"],
            ),
            description="List of edits to apply. Each edit specifies a file and the change to make.",
            min_items=1,
            max_items=20,
        ),
        dry_run=BooleanSchema(
            description="Validate and summarize the patch without writing files.",
            default=False,
        ),
        required=["edits"],
    )
)
class ApplyPatchTool(_FsTool):
    """根据结构化编辑列表应用补丁。"""
    _scopes = {"core", "subagent"}

    @property
    def name(self) -> str:
        return "apply_patch"

    @property
    def description(self) -> str:
        return (
            "Default tool for code edits. Supports multi-file changes in a single call. "
            "Provide a list of structured edits, each specifying a file path, action "
            "(replace/add), and the exact text to change. "
            "Paths must be relative. Set dry_run=true to validate and preview without writing files. "
            "Use edit_file only for small exact replacements on a single file."
        )

    async def execute(
        self,
        edits: list[dict] | None = None,
        dry_run: bool = False,
        **kwargs: Any,
    ) -> str:
        """校验并应用一组文件修改；失败时尽量回滚。"""
        try:
            if not edits:
                raise _PatchError("must provide edits")

            writes: dict[Path, str] = {}
            summaries: list[_PatchSummary] = []

            for edit in edits:
                if not isinstance(edit, dict):
                    raise _PatchError("each edit must be an object")
                raw_path = edit.get("path")
                if not isinstance(raw_path, str):
                    raise _PatchError("path required for edit")
                path = _validate_relative_path(raw_path)
                action = edit.get("action")
                if not isinstance(action, str):
                    raise _PatchError(f"action required for edit: {path}")
                source = self._resolve(path)

                if action == "add":
                    # add 的语义是“新建文件”或“向现有文件追加内容”。
                    new_text = edit.get("new_text")
                    if new_text is None:
                        raise _PatchError(f"new_text required for add: {path}")

                    pending = writes.get(source)
                    if pending is not None:
                        content = pending
                        exists = True
                    elif source.exists():
                        raw = source.read_bytes()
                        try:
                            content = raw.decode("utf-8")
                        except UnicodeDecodeError:
                            raise _PatchError(f"file is not UTF-8 text: {path}")
                        exists = True
                    else:
                        content = ""
                        exists = False

                    if exists:
                        uses_crlf = "\r\n" in content
                        new_norm = _append_text(content, new_text)
                        if uses_crlf:
                            new_norm = new_norm.replace("\n", "\r\n")
                        writes[source] = new_norm
                        added, deleted = _line_diff_stats(content, new_norm)
                        action_name = "update"
                    else:
                        new_norm = new_text.replace("\r\n", "\n")
                        if new_norm and not new_norm.endswith("\n"):
                            new_norm += "\n"
                        writes[source] = new_norm
                        added = _text_line_count(new_norm)
                        deleted = 0
                        action_name = "add"

                    summaries.append(
                        _PatchSummary(
                            action=action_name, path=path, added=added, deleted=deleted
                        )
                    )

                elif action == "replace":
                    # replace 要求 old_text 精确存在且只出现一次，
                    # 否则宁可失败，也不冒险改错位置。
                    old_text = edit.get("old_text") or ""
                    if not old_text:
                        raise _PatchError(f"old_text required for replace: {path}")
                    new_text = edit.get("new_text")
                    if new_text is None:
                        raise _PatchError(f"new_text required for replace: {path}")

                    pending = writes.get(source)
                    if pending is not None:
                        content = pending
                    elif source.exists():
                        raw = source.read_bytes()
                        try:
                            content = raw.decode("utf-8")
                        except UnicodeDecodeError:
                            raise _PatchError(f"file is not UTF-8 text: {path}")
                    else:
                        raise _PatchError(f"file to update does not exist: {path}")

                    if pending is None and not source.is_file():
                        raise _PatchError(f"path to update is not a file: {path}")

                    uses_crlf = "\r\n" in content
                    norm_content = content.replace("\r\n", "\n")
                    norm_old = old_text.replace("\r\n", "\n")

                    pos = norm_content.find(norm_old)
                    if pos < 0:
                        raise _PatchError(f"old_text not found in {path}")
                    if norm_content.find(norm_old, pos + 1) >= 0:
                        raise _PatchError(f"old_text appears multiple times in {path}")

                    new_norm = (
                        norm_content[:pos]
                        + new_text.replace("\r\n", "\n")
                        + norm_content[pos + len(norm_old) :]
                    )
                    if new_norm and not new_norm.endswith("\n"):
                        new_norm += "\n"
                    if uses_crlf:
                        new_norm = new_norm.replace("\n", "\r\n")

                    writes[source] = new_norm
                    added, deleted = _line_diff_stats(content, new_norm)
                    summaries.append(
                        _PatchSummary(
                            action="update", path=path, added=added, deleted=deleted
                        )
                    )

                else:
                    raise _PatchError(f"unknown action: {action}")

            if dry_run:
                # dry_run 只做校验和摘要预览，不真正写盘。
                return "Patch dry-run succeeded:\n" + "\n".join(
                    _format_summary(summary) for summary in summaries
                )

            backups: dict[Path, bytes | None] = {}
            for path in writes:
                backups[path] = path.read_bytes() if path.exists() else None

            try:
                for path, content in writes.items():
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text(content, encoding="utf-8", newline="")
            except Exception:
                # 多文件写入中途失败时，尽量恢复到修改前状态，
                # 让这次补丁尽可能接近“全成或全不成”。
                for path, data in backups.items():
                    if data is None:
                        if path.exists():
                            path.unlink()
                    else:
                        path.parent.mkdir(parents=True, exist_ok=True)
                        path.write_bytes(data)
                raise

            for path in writes:
                self._file_states.record_write(path)
            return "Patch applied:\n" + "\n".join(
                _format_summary(summary) for summary in summaries
            )
        except PermissionError as exc:
            return f"Error: {exc}"
        except _PatchError as exc:
            return f"Error applying patch: {exc}"
        except Exception as exc:
            return f"Error applying patch: {exc}"
