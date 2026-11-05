"""Consultas sobre combinaciones de etiquetas.

Gramática, de menor a mayor precedencia (las palabras clave no distinguen
mayúsculas):

    or   := and ("OR" and)*
    and  := not ("AND" not)*
    not  := "NOT" not | atom
    atom := TAG | "(" or ")"

Una consulta se resuelve en dos pasos: candidates() usa el índice para sacar
los posibles archivos y matches() decide con las etiquetas reales de los
metadatos. El índice puede estar atrasado, así que NOT se evalúa solo en
matches(): restar resultados del índice podría ocultar un archivo válido.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Callable, Collection, Iterable

from tagfs.model import RESERVED_WORDS, FileId, InvalidQuery, normalize_tag


@dataclass(frozen=True)
class Tag:
    name: str


@dataclass(frozen=True)
class Not:
    item: Expr


@dataclass(frozen=True)
class And:
    items: tuple[Expr, ...]


@dataclass(frozen=True)
class Or:
    items: tuple[Expr, ...]


Expr = Tag | Not | And | Or

_TOKEN = re.compile(r"[()]|[^\s()]+")


class _Parser:
    def __init__(self, text: str) -> None:
        self._tokens = _TOKEN.findall(text)
        self._position = 0

    def parse(self) -> Expr:
        expr = self._or()
        token = self._peek()
        if token is not None:
            raise InvalidQuery(f"Símbolo inesperado: {token!r}")
        return expr

    def _peek(self) -> str | None:
        if self._position < len(self._tokens):
            return self._tokens[self._position]
        return None

    def _next(self) -> str | None:
        token = self._peek()
        self._position += 1
        return token

    def _accept(self, keyword: str) -> bool:
        token = self._peek()
        if token is not None and token.lower() == keyword:
            self._position += 1
            return True
        return False

    def _or(self) -> Expr:
        items = [self._and()]
        while self._accept("or"):
            items.append(self._and())
        return items[0] if len(items) == 1 else Or(tuple(items))

    def _and(self) -> Expr:
        items = [self._not()]
        while self._accept("and"):
            items.append(self._not())
        return items[0] if len(items) == 1 else And(tuple(items))

    def _not(self) -> Expr:
        if self._accept("not"):
            return Not(self._not())
        return self._atom()

    def _atom(self) -> Expr:
        token = self._next()
        if token is None:
            raise InvalidQuery("La consulta está incompleta")
        if token == "(":
            expr = self._or()
            if self._next() != ")":
                raise InvalidQuery("Falta un ')'")
            return expr
        if token == ")" or token.lower() in RESERVED_WORDS:
            raise InvalidQuery(f"Símbolo inesperado: {token!r}")
        return Tag(normalize_tag(token))


def parse_query(text: str) -> Expr:
    """Convierte el string en un árbol o lanza InvalidQuery."""
    try:
        expr = _Parser(text).parse()
    except RecursionError:
        raise InvalidQuery("La consulta está demasiado anidada") from None
    if not is_bounded(expr):
        raise InvalidQuery(f"La consulta necesita una parte positiva: {text!r}")
    return expr


def is_bounded(expr: Expr) -> bool:
    """Indica si el índice puede dar candidatos para la expresión."""
    match expr:
        case Tag():
            return True
        case And(items):
            return any(is_bounded(item) for item in items)
        case Or(items):
            return all(is_bounded(item) for item in items)
    return False


def candidates(expr: Expr, lookup: Callable[[str], Iterable[FileId]]) -> set[FileId]:
    """Devuelve los posibles archivos según el índice."""
    match expr:
        case Tag(name):
            return set(lookup(name))
        case Or(items):
            return set().union(*(candidates(item, lookup) for item in items))
        case And(items):
            bounded = (item for item in items if is_bounded(item))
            return set.intersection(*(candidates(item, lookup) for item in bounded))
    raise InvalidQuery("La consulta necesita una parte positiva")


def matches(expr: Expr, tags: Collection[str]) -> bool:
    """Evalúa la expresión con las etiquetas reales de un archivo."""
    match expr:
        case Tag(name):
            return name in tags
        case Not(item):
            return not matches(item, tags)
        case And(items):
            return all(matches(item, tags) for item in items)
        case Or(items):
            return any(matches(item, tags) for item in items)
    raise TypeError(f"Expresión desconocida: {expr!r}")
