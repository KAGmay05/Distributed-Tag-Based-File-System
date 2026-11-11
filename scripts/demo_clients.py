"""Demo: varios clientes usan el sistema a la vez sobre actores de Ray.

Uso: python scripts/demo_clients.py
"""

import os
import time

from tagfs.query import matches, parse_query
from tagfs.ray_runtime import ActorProxy, run_in_workers, start_cluster, stop_cluster
from tagfs.router import shard_for

CLIENTS = 4
FILES_PER_CLIENT = 25
KINDS = ["foto", "video", "doc"]
YEARS = ["2023", "2024"]
DRAFT = "borrador"
QUERIES = [
    "foto",
    "foto AND 2024",
    "foto OR video",
    "(foto OR video) AND 2024 AND NOT borrador",
]


def client(cluster, client_id):
    started = time.time()
    gateway = cluster.gateway()
    created = []
    for i in range(FILES_PER_CLIENT):
        tags = {KINDS[i % len(KINDS)], YEARS[i % len(YEARS)]}
        if i % 5 == 0:
            tags.add(DRAFT)
        content = f"cliente {client_id}, archivo {i}".encode()
        created.append(gateway.put(f"cliente{client_id}-{i}.dat", content, tags))
    return {"pid": os.getpid(), "started": started, "finished": time.time(), "created": created}


def print_processes(cluster, results):
    print("Procesos")
    print(f"  script de la demo: {os.getpid()}")
    for index, shard in enumerate(cluster.metadata):
        print(f"  MetadataShard {index}: {shard.pid()}")
    for index, shard in enumerate(cluster.tags):
        print(f"  TagShard {index}: {shard.pid()}")
    for name, node in cluster.storage.items():
        print(f"  StorageNode {name}: {ActorProxy(node).pid()}")
    for index, result in enumerate(results):
        print(f"  cliente {index}: {result['pid']}")


def print_concurrency(results):
    last_start = max(result["started"] for result in results)
    first_finish = min(result["finished"] for result in results)
    total = max(result["finished"] for result in results) - min(r["started"] for r in results)
    print("\nConcurrencia")
    print(f"  {CLIENTS} clientes crearon {CLIENTS * FILES_PER_CLIENT} archivos en {total:.2f} s")
    print(f"  todos activos a la vez durante {max(0, first_finish - last_start):.2f} s")


def print_distribution(cluster):
    print("\nReparto")
    for index, shard in enumerate(cluster.metadata):
        print(f"  MetadataShard {index}: {len(shard.list_ids())} archivos")
    for tag in [*KINDS, *YEARS, DRAFT]:
        index = shard_for(tag, len(cluster.tags))
        print(f"  etiqueta {tag!r} en TagShard {index}: {len(cluster.tags[index].lookup(tag))} archivos")


def check_queries(cluster, gateway):
    """Compara cada consulta con el resultado calculado directamente desde los metadatos."""
    files = []
    for shard in cluster.metadata:
        files.extend(shard.get_many(shard.list_ids()).values())

    print("\nConsultas")
    for text in QUERIES:
        expr = parse_query(text)
        expected = sorted(meta.file_id for meta in files if matches(expr, meta.tags))
        found = gateway.query(text)
        print(f"  {text}: {len(found)} archivos")
        if found != expected:
            raise SystemExit(f"La consulta {text!r} no coincide con los metadatos")


def show_operations(gateway):
    print("\nOperaciones")
    file_id = gateway.put("nota.txt", b"hola", {"Demo"})
    print(f"  put: {file_id}")
    print(f"  get: {gateway.get(file_id)}")
    print(f"  add_tags: version {gateway.add_tags(file_id, {'urgente'})}")
    print(f"  remove_tags: version {gateway.remove_tags(file_id, {'demo'})}")
    print(f"  rename: version {gateway.rename(file_id, 'nota-final.txt')}")
    print(f"  get: {gateway.get(file_id)}")
    print(f"  query('urgente'): {gateway.query('urgente')}")
    gateway.delete(file_id)
    print(f"  delete, y luego query('urgente'): {gateway.query('urgente')}")


def main():
    cluster = start_cluster()
    results = run_in_workers(client, [(cluster, i) for i in range(CLIENTS)])

    gateway = cluster.gateway()
    created = [file_id for result in results for file_id in result["created"]]
    if gateway.list() != sorted(created):
        raise SystemExit("list() no devuelve exactamente los archivos creados")

    print_processes(cluster, results)
    print_concurrency(results)
    print_distribution(cluster)
    check_queries(cluster, gateway)
    show_operations(gateway)

    stop_cluster()
    print("\nDemo correcta")


if __name__ == "__main__":
    main()
