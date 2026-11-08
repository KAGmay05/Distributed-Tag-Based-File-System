"""Tipos compartidos por los shards de metadatos y de etiquetas."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, NamedTuple

FileId = str

RESERVED_WORDS = frozenset({"and", "or", "not"})


class TagFSError(Exception):
    """Base de los errores del sistema."""


class FileNotFound(TagFSError):
    """El archivo no existe o fue eliminado."""


class FileIdConflict(TagFSError):
    """El ID ya se usó con otros datos; los IDs nunca se reutilizan."""


class ContentNotFound(TagFSError):
    """El almacén no tiene contenido con ese hash."""


class ContentIntegrityError(TagFSError):
    """El contenido no coincide con su hash."""


class InvalidTag(TagFSError, ValueError):
    """La etiqueta no se podría escribir en una consulta."""


class InvalidQuery(TagFSError, ValueError):
    """La consulta tiene un error de sintaxis o no tiene parte positiva."""


@dataclass(frozen=True)
class FileMeta:
    file_id: FileId
    name: str
    tags: frozenset[str]
    content_hash: str
    version: int = 1
    deleted: bool = False


@dataclass(frozen=True)
class IndexOp:
    """Cambio pendiente de aplicar en el índice de una etiqueta."""

    tag: str
    file_id: FileId
    present: bool
    version: int


class Mutation(NamedTuple):
    """Resultado de un cambio de metadatos."""

    version: int
    index_ops: tuple[IndexOp, ...]


def normalize_tag(tag: str) -> str:
    """Devuelve la etiqueta en minúsculas o lanza InvalidTag.

    Las reglas garantizan que toda etiqueta guardada se pueda escribir en una
    consulta: sin espacios ni paréntesis, y sin usar palabras reservadas.
    """
    normalized = tag.lower()
    if (
        not normalized
        or normalized in RESERVED_WORDS
        or any(char.isspace() or char in "()" for char in normalized)
    ):
        raise InvalidTag(f"Etiqueta no válida: {tag!r}")
    return normalized


def normalize_tags(tags: Iterable[str]) -> frozenset[str]:
    return frozenset(normalize_tag(tag) for tag in tags)
