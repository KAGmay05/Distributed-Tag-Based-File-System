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


class InvalidTag(TagFSError, ValueError):
    """La etiqueta no se podría escribir en una consulta."""


class InvalidQuery(TagFSError, ValueError):
    """La consulta tiene un error de sintaxis o no tiene parte positiva."""


class ServiceUnavailable(TagFSError):
    """Una parte del sistema no respondió: murió o no contestó a tiempo.

    Si ocurre durante un cambio, el cambio puede haberse hecho o no: un shard
    colgado puede aplicarlo cuando vuelva a responder.
    """


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


class FileInfo(NamedTuple):
    """Datos de un archivo sin su contenido, tal como los recibe el cliente."""

    file_id: FileId
    name: str
    tags: frozenset[str]
    version: int


class BulkResult(NamedTuple):
    """Resultado de un cambio sobre todos los archivos que cumplen una consulta."""

    # Los que cumplían la consulta, con su versión resultante. Incluye los que ya
    # estaban como se pedía, igual que add_tags devuelve la versión sin cambiarla.
    done: dict[FileId, int]
    # Posibles archivos cuyo shard no respondió: no se sabe si se cambiaron.
    unknown: list[FileId]


def normalize_tag(tag: str) -> str:
    """Devuelve la etiqueta en minúsculas o lanza InvalidTag.

    Las reglas garantizan que toda etiqueta guardada se pueda escribir en una
    consulta: sin espacios ni paréntesis, y sin usar palabras reservadas.
    """
    if not isinstance(tag, str):
        raise InvalidTag(f"Una etiqueta debe ser texto: {tag!r}")
    normalized = tag.lower()
    if (
        not normalized
        or normalized in RESERVED_WORDS
        or any(char.isspace() or char in "()" for char in normalized)
    ):
        raise InvalidTag(f"Etiqueta no válida: {tag!r}")
    return normalized


def normalize_tags(tags: Iterable[str]) -> frozenset[str]:
    """Normaliza un conjunto de etiquetas o lanza InvalidTag."""
    # Un texto también se puede recorrer: "foto" se guardaría como f, o y t.
    if isinstance(tags, str):
        raise InvalidTag(f"Se espera un conjunto de etiquetas, no un texto: {tags!r}")
    return frozenset(normalize_tag(tag) for tag in tags)
