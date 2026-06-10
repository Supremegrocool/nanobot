"""文档解析工具。

【中文名称】文档解析工具

【功能说明】
负责识别 PDF、DOCX、PPTX、Excel、纯文本等文件类型，并提取可供 Agent 阅读的文本内容。

【在整体架构中的位置】
该文件属于 P1 范围的通用工具代码：它不改变 Agent 主循环的骨架，
而是负责把某一种外部协议、模型接口或通用能力接到 nanobot 的统一抽象上。

【学习重点】
- 先看本文件的配置类/数据类，理解外部服务需要哪些参数。
- 再看 start/stop/send 或 generate/stream 等入口方法，理解数据如何进出。
- 最后看私有辅助函数，它们通常是在处理平台限制、协议兼容或安全边界。"""

import mimetypes
from pathlib import Path

from loguru import logger

from nanobot.utils.helpers import detect_image_mime

# 说明：这里处理 文档解析工具 的协议细节或边界情况，避免外部差异影响核心流程。
SUPPORTED_EXTENSIONS: set[str] = {
    # 说明：这里处理 文档解析工具 的协议细节或边界情况，避免外部差异影响核心流程。
    ".pdf",
    ".docx",
    ".xlsx",
    ".pptx",
    # 说明：这里处理 文档解析工具 的协议细节或边界情况，避免外部差异影响核心流程。
    ".txt",
    ".md",
    ".csv",
    ".json",
    ".xml",
    ".html",
    ".htm",
    ".log",
    ".yaml",
    ".yml",
    ".toml",
    ".ini",
    ".cfg",
    # 说明：这里处理 文档解析工具 的协议细节或边界情况，避免外部差异影响核心流程。
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".webp",
}

_MAX_TEXT_LENGTH = 200_000


def extract_text(path: Path) -> str | None:
    """执行 `extract_text`。

    【中文名称】extract_text

    【功能说明】
    这是 文档解析工具 中的一个步骤函数，用来支撑：负责识别 PDF、DOCX、PPTX、Excel、纯文本等文件类型，并提取可供 Agent 阅读的文本内容。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - path: 调用方传入的 `path` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    if not isinstance(path, Path):
        path = Path(path)

    if not path.exists():
        return f"[error: file not found: {path}]"

    ext = path.suffix.lower()

    # 说明：这里处理 文档解析工具 的协议细节或边界情况，避免外部差异影响核心流程。
    # 说明：这里处理 文档解析工具 的协议细节或边界情况，避免外部差异影响核心流程。
    # 说明：这里处理 文档解析工具 的协议细节或边界情况，避免外部差异影响核心流程。
    if ext == ".pdf":
        return _extract_pdf(path)
    elif ext == ".docx":
        return _extract_docx(path)
    elif ext == ".xlsx":
        return _extract_xlsx(path)
    elif ext == ".pptx":
        return _extract_pptx(path)
    elif _is_text_extension(ext):
        return _extract_text_file(path)
    elif ext in {".png", ".jpg", ".jpeg", ".gif", ".webp"}:
        # 说明：这里处理 文档解析工具 的协议细节或边界情况，避免外部差异影响核心流程。
        return f"[image: {path.name}]"
    else:
        # 说明：这里处理 文档解析工具 的协议细节或边界情况，避免外部差异影响核心流程。
        return None


def _extract_pdf(path: Path) -> str:
    """执行 `_extract_pdf`。

    【中文名称】_extract_pdf

    【功能说明】
    这是 文档解析工具 中的一个步骤函数，用来支撑：负责识别 PDF、DOCX、PPTX、Excel、纯文本等文件类型，并提取可供 Agent 阅读的文本内容。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - path: 调用方传入的 `path` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    try:
        from pypdf import PdfReader
    except ImportError:
        return "[error: pypdf not installed]"
    try:
        reader = PdfReader(path)
        pages: list[str] = []
        for i, page in enumerate(reader.pages, 1):
            text = page.extract_text() or ""
            pages.append(f"--- Page {i} ---\n{text}")
        return _truncate("\n\n".join(pages), _MAX_TEXT_LENGTH)
    except Exception as e:
        logger.exception("Failed to extract PDF {}", path)
        return f"[error: failed to extract PDF: {e!s}]"


def _extract_docx(path: Path) -> str:
    """执行 `_extract_docx`。

    【中文名称】_extract_docx

    【功能说明】
    这是 文档解析工具 中的一个步骤函数，用来支撑：负责识别 PDF、DOCX、PPTX、Excel、纯文本等文件类型，并提取可供 Agent 阅读的文本内容。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - path: 调用方传入的 `path` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    try:
        from docx import Document as DocxDocument
    except ImportError:
        return "[error: python-docx not installed]"
    try:
        doc = DocxDocument(path)
        paragraphs: list[str] = [p.text for p in doc.paragraphs if p.text.strip()]
        return _truncate("\n\n".join(paragraphs), _MAX_TEXT_LENGTH)
    except Exception as e:
        logger.exception("Failed to extract DOCX {}", path)
        return f"[error: failed to extract DOCX: {e!s}]"


def _extract_xlsx(path: Path) -> str:
    """执行 `_extract_xlsx`。

    【中文名称】_extract_xlsx

    【功能说明】
    这是 文档解析工具 中的一个步骤函数，用来支撑：负责识别 PDF、DOCX、PPTX、Excel、纯文本等文件类型，并提取可供 Agent 阅读的文本内容。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - path: 调用方传入的 `path` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    try:
        from openpyxl import load_workbook
    except ImportError:
        return "[error: openpyxl not installed]"
    try:
        wb = load_workbook(path, read_only=True, data_only=True)
        try:
            sheets: list[str] = []
            for sheet_name in wb.sheetnames:
                ws = wb[sheet_name]
                rows: list[str] = []
                for row in ws.iter_rows(values_only=True):
                    row_text = "\t".join(str(cell) if cell is not None else "" for cell in row)
                    if row_text.strip():
                        rows.append(row_text)
                if rows:
                    sheets.append(f"--- Sheet: {sheet_name} ---\n" + "\n".join(rows))
            return _truncate("\n\n".join(sheets), _MAX_TEXT_LENGTH)
        finally:
            wb.close()
    except Exception as e:
        logger.exception("Failed to extract XLSX {}", path)
        return f"[error: failed to extract XLSX: {e!s}]"


def _extract_pptx(path: Path) -> str:
    """执行 `_extract_pptx`。

    【中文名称】_extract_pptx

    【功能说明】
    这是 文档解析工具 中的一个步骤函数，用来支撑：负责识别 PDF、DOCX、PPTX、Excel、纯文本等文件类型，并提取可供 Agent 阅读的文本内容。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - path: 调用方传入的 `path` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    try:
        from pptx import Presentation as PptxPresentation
    except ImportError:
        return "[error: python-pptx not installed]"
    try:
        prs = PptxPresentation(path)
        slides: list[str] = []
        for i, slide in enumerate(prs.slides, 1):
            slide_text: list[str] = []
            for shape in slide.shapes:
                _collect_pptx_shape_text(shape, slide_text)
            if slide_text:
                slides.append(f"--- Slide {i} ---\n" + "\n".join(slide_text))
        return _truncate("\n\n".join(slides), _MAX_TEXT_LENGTH)
    except Exception as e:
        logger.exception("Failed to extract PPTX {}", path)
        return f"[error: failed to extract PPTX: {e!s}]"


def _collect_pptx_shape_text(shape, out: list[str]) -> None:
    """执行 `_collect_pptx_shape_text`。

    【中文名称】_collect_pptx_shape_text

    【功能说明】
    这是 文档解析工具 中的一个步骤函数，用来支撑：负责识别 PDF、DOCX、PPTX、Excel、纯文本等文件类型，并提取可供 Agent 阅读的文本内容。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - shape: 调用方传入的 `shape` 数据；具体类型以函数签名为准。
    - out: 调用方传入的 `out` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    sub_shapes = getattr(shape, "shapes", None)
    if sub_shapes is not None:
        for sub in sub_shapes:
            _collect_pptx_shape_text(sub, out)
        return

    if getattr(shape, "has_table", False):
        for row in shape.table.rows:
            cells = [cell.text.strip() for cell in row.cells]
            line = "\t".join(cell for cell in cells if cell)
            if line:
                out.append(line)
        return

    text = getattr(shape, "text", "")
    if text:
        out.append(text)


def _extract_text_file(path: Path) -> str:
    """执行 `_extract_text_file`。

    【中文名称】_extract_text_file

    【功能说明】
    这是 文档解析工具 中的一个步骤函数，用来支撑：负责识别 PDF、DOCX、PPTX、Excel、纯文本等文件类型，并提取可供 Agent 阅读的文本内容。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - path: 调用方传入的 `path` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    try:
        # 说明：这里处理 文档解析工具 的协议细节或边界情况，避免外部差异影响核心流程。
        try:
            content = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            content = path.read_text(encoding="latin-1")
        return _truncate(content, _MAX_TEXT_LENGTH)
    except Exception as e:
        logger.exception("Failed to read text file {}", path)
        return f"[error: failed to read file: {e!s}]"


def _truncate(text: str, max_length: int) -> str:
    """执行 `_truncate`。

    【中文名称】_truncate

    【功能说明】
    这是 文档解析工具 中的一个步骤函数，用来支撑：负责识别 PDF、DOCX、PPTX、Excel、纯文本等文件类型，并提取可供 Agent 阅读的文本内容。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - text: 调用方传入的 `text` 数据；具体类型以函数签名为准。
    - max_length: 调用方传入的 `max_length` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    if len(text) <= max_length:
        return text
    return text[:max_length] + f"... (truncated, {len(text)} chars total)"


def _is_text_extension(ext: str) -> bool:
    """执行 `_is_text_extension`。

    【中文名称】_is_text_extension

    【功能说明】
    这是 文档解析工具 中的一个步骤函数，用来支撑：负责识别 PDF、DOCX、PPTX、Excel、纯文本等文件类型，并提取可供 Agent 阅读的文本内容。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - ext: 调用方传入的 `ext` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    return ext in {
        ".txt",
        ".md",
        ".csv",
        ".json",
        ".xml",
        ".html",
        ".htm",
        ".log",
        ".yaml",
        ".yml",
        ".toml",
        ".ini",
        ".cfg",
    }


# 说明：这里处理 文档解析工具 的协议细节或边界情况，避免外部差异影响核心流程。
# 说明：这里处理 文档解析工具 的协议细节或边界情况，避免外部差异影响核心流程。
# 说明：这里处理 文档解析工具 的协议细节或边界情况，避免外部差异影响核心流程。

_MAX_EXTRACT_FILE_SIZE = 50 * 1024 * 1024  # 说明：这里处理 文档解析工具 的协议细节或边界情况，避免外部差异影响核心流程。


def is_image_file(path: str) -> bool:
    """执行 `is_image_file`。

    【中文名称】is_image_file

    【功能说明】
    这是 文档解析工具 中的一个步骤函数，用来支撑：负责识别 PDF、DOCX、PPTX、Excel、纯文本等文件类型，并提取可供 Agent 阅读的文本内容。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - path: 调用方传入的 `path` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    p = Path(path)
    mime: str | None = None
    if p.is_file():
        try:
            with p.open("rb") as f:
                mime = detect_image_mime(f.read(16))
        except OSError:
            mime = None
    if not mime:
        mime = mimetypes.guess_type(path)[0]
    return bool(mime and mime.startswith("image/"))


def reference_non_image_attachments(
    content: str, media: list[str],
) -> tuple[str, list[str]]:
    """执行 `reference_non_image_attachments`。

    【中文名称】reference_non_image_attachments

    【功能说明】
    这是 文档解析工具 中的一个步骤函数，用来支撑：负责识别 PDF、DOCX、PPTX、Excel、纯文本等文件类型，并提取可供 Agent 阅读的文本内容。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - content: 调用方传入的 `content` 数据；具体类型以函数签名为准。
    - media: 调用方传入的 `media` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    image_paths: list[str] = []
    attachment_refs: list[str] = []
    for path in media:
        if is_image_file(path):
            image_paths.append(path)
        else:
            attachment_refs.append(f"[Attachment: {path}]")
    if attachment_refs:
        suffix = "\n".join(attachment_refs)
        content = f"{content}\n\n{suffix}" if content else suffix
    return content, image_paths


def extract_documents(
    text: str,
    media_paths: list[str],
    *,
    max_file_size: int = _MAX_EXTRACT_FILE_SIZE,
) -> tuple[str, list[str]]:
    """执行 `extract_documents`。

    【中文名称】extract_documents

    【功能说明】
    这是 文档解析工具 中的一个步骤函数，用来支撑：负责识别 PDF、DOCX、PPTX、Excel、纯文本等文件类型，并提取可供 Agent 阅读的文本内容。
    阅读时可以把它看作“把上游传入的数据整理、校验或转换后，再交给下一层”的小环节。

    【参数说明】
    - text: 调用方传入的 `text` 数据；具体类型以函数签名为准。
    - media_paths: 调用方传入的 `media_paths` 数据；具体类型以函数签名为准。
    - max_file_size: 调用方传入的 `max_file_size` 数据；具体类型以函数签名为准。

    【返回值】
    - 返回当前步骤的处理结果；如果没有显式返回值，则表示只完成状态更新、发送消息或副作用操作。"""
    image_paths: list[str] = []
    doc_texts: list[str] = []

    for path_str in media_paths:
        p = Path(path_str)
        if not p.is_file():
            continue

        try:
            size = p.stat().st_size
        except OSError:
            continue
        if size > max_file_size:
            logger.warning(
                "Skipping oversized file for extraction: {} ({:.1f} MB > {} MB limit)",
                p.name, size / (1024 * 1024), max_file_size // (1024 * 1024),
            )
            continue

        if is_image_file(path_str):
            image_paths.append(path_str)
        else:
            extracted = extract_text(p)
            if extracted and not extracted.startswith("[error:"):
                doc_texts.append(f"[File: {p.name}]\n{extracted}")

    if doc_texts:
        text = text + "\n\n" + "\n\n".join(doc_texts)

    return text, image_paths
