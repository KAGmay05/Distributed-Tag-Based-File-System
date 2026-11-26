"""Identificacion del contenido del archivo por su hash"""

from __future__ import annotations
import hashlib
from tagfs.model import TagFSError

class ContentNotFound(TagFSError):
    """Ningún nodo tiene el contenido con ese hash."""

class ContentCorrupted(TagFSError):
    """Los bytes recibidos no coinciden con su hash."""

class StorageUnavailable(TagFSError):
    """No quedan nodos vivos donde guardar el contenido."""

class NodeUnreachable(TagFSError):
    """El nodo está vivo pero aislado por una partición de red."""

def content_hash(data: bytes) -> str:
    """Devuelve el hash del contenido"""
    if not isinstance(data, bytes):
        raise TypeError(f"Se esperan bytes, no {type(data).__name__}")
    return hashlib.sha256(data).hexdigest()