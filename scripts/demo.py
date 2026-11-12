"""Demostración del sistema completo: operaciones, eliminación, fallos,
particiones y concurrencia.

Uso, desde la raíz del proyecto:  python scripts/demo.py
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.stdout.reconfigure(encoding="utf-8")  # la consola de Windows no siempre usa UTF-8
# Al leer, se usa la primera réplica que responde y las demás respuestas no se leen.
# Si alguna era de un almacén aislado, Ray la imprimiría como error sin atender.
os.environ.setdefault("RAY_IGNORE_UNHANDLED_ERRORS", "1")

import ray  # noqa: E402

from tagfs.content import ContentNotFound, NodeUnreachable, content_hash  # noqa: E402
from tagfs.gateway import Gateway  # noqa: E402
from tagfs.ray_runtime import RayCluster, run_in_workers, start_cluster, start_ray  # noqa: E402

CLIENTS = 50


def title(text: str) -> None:
    print(f"\n{'=' * 64}\n {text}\n{'=' * 64}")


def step(text: str) -> None:
    print(f"\n>>> {text}")


def show(label: str, value: object) -> None:
    print(f"    {label:<42} {value}")


def copies(cluster: RayCluster, content: bytes) -> int:
    """Cuántos almacenes vivos guardan ese contenido."""
    count = 0
    for node in cluster.storage.values():
        try:
            count += ray.get(node.has.remote(content_hash(content)), timeout=10)
        except (ray.exceptions.RayActorError, NodeUnreachable):
            pass
    return count


def misplaced(cluster: RayCluster, gateway: Gateway) -> int:
    """Cuántos contenidos vivos no están exactamente en las réplicas que dice el anillo."""
    count = 0
    for meta in gateway._live_files():
        key = meta.content_hash
        holders = {name for name, node in cluster.storage.items() if ray.get(node.has.remote(key))}
        count += holders != set(gateway._content.replicas(key))
    return count


def names(gateway: Gateway, file_ids: list[str]) -> str:
    return ", ".join(gateway.get(file_id).name for file_id in file_ids) or "(ninguno)"


def readable(gateway: Gateway, file_ids: list[str]) -> int:
    count = 0
    for file_id in file_ids:
        try:
            gateway.get(file_id)
            count += 1
        except ContentNotFound:
            pass
    return count


def demo_operations(cluster: RayCluster, gateway: Gateway) -> None:
    title("1. Guardar archivos con etiquetas")
    files = {
        "playa.jpg": (b"foto de la playa", {"foto", "viaje", "2024"}),
        "copia-playa.jpg": (b"foto de la playa", {"foto", "backup"}),
        "montana.jpg": (b"foto de la montana", {"foto", "viaje", "2023"}),
        "cumple.mp4": (b"video del cumple", {"video", "familia", "2024"}),
        "informe.pdf": (b"informe trimestral", {"trabajo", "2024", "borrador"}),
        "cancion.mp3": (b"una cancion", {"musica"}),
    }
    ids = {}
    for name, (content, tags) in files.items():
        ids[name] = gateway.put(name, content, tags)
        show(name, f"id {ids[name][:8]}...  etiquetas: {', '.join(sorted(tags))}")
    show("Archivos en el sistema (list)", len(gateway.list()))
    show("Copias de 'foto de la playa' (2 archivos)", copies(cluster, b"foto de la playa"))
    print("    → el mismo contenido se guarda una sola vez, en 2 almacenes")

    title("2. Leer un archivo")
    stored = gateway.get(ids["playa.jpg"])
    show("Nombre", stored.name)
    show("Contenido", stored.content)
    show("Etiquetas", ", ".join(sorted(stored.tags)))
    show("Versión", stored.version)

    title("3. Buscar por combinaciones de etiquetas")
    for expr in [
        "foto",
        "foto AND viaje",
        "foto AND NOT 2023",
        "video OR musica",
        "2024 AND NOT borrador",
        "(foto OR video) AND 2024",
    ]:
        show(expr, names(gateway, gateway.query(expr)))

    title("4. Modificar etiquetas y nombre (cada cambio sube la versión)")
    show("add_tags(informe.pdf, final)", f"versión {gateway.add_tags(ids['informe.pdf'], {'final'})}")
    show("remove_tags(informe.pdf, borrador)", f"versión {gateway.remove_tags(ids['informe.pdf'], {'borrador'})}")
    show("rename(montana.jpg → cordillera.jpg)", f"versión {gateway.rename(ids['montana.jpg'], 'cordillera.jpg')}")
    show("add_tags(informe.pdf, final) otra vez", f"versión {gateway.add_tags(ids['informe.pdf'], {'final'})}  (sin cambios)")
    show("2024 AND NOT borrador", names(gateway, gateway.query("2024 AND NOT borrador")))
    show("foto AND viaje", names(gateway, gateway.query("foto AND viaje")))

    title("5. Eliminar y recolectar basura")
    step("Se elimina playa.jpg, que comparte contenido con copia-playa.jpg")
    gateway.delete(ids["playa.jpg"])
    show("Recolección de basura: contenidos borrados", gateway.collect_garbage())
    show("Copias de 'foto de la playa'", copies(cluster, b"foto de la playa"))
    show("copia-playa.jpg se sigue leyendo", gateway.get(ids["copia-playa.jpg"]).content)
    step("Se elimina copia-playa.jpg: ya nadie usa ese contenido")
    gateway.delete(ids["copia-playa.jpg"])
    show("Recolección de basura: contenidos borrados", gateway.collect_garbage())
    show("Copias de 'foto de la playa'", copies(cluster, b"foto de la playa"))
    show("Archivos en el sistema (list)", len(gateway.list()))


def demo_failures(cluster: RayCluster, gateway: Gateway) -> None:
    title("6. Se cae un almacén (2 copias de cada contenido)")
    ids = [gateway.put(f"archivo-{i}.txt", f"contenido {i}".encode(), {"lote"}) for i in range(20)]
    victim = gateway._content.replicas(content_hash(b"contenido 0"))[0]
    show("Archivos guardados", len(ids))
    step(f"Se apaga el almacén {victim}")
    ray.kill(cluster.storage[victim])
    show("Archivos que se pueden leer", f"{readable(gateway, ids)} de {len(ids)}")
    new = gateway.put("despues.txt", b"escrito con un almacen caido", {"lote"})
    show("Archivo nuevo, copias guardadas", copies(cluster, b"escrito con un almacen caido"))
    show("Se lee el archivo nuevo", gateway.get(new).content)

    title("7. Lo mismo, pero con 1 sola copia de cada contenido")
    single = start_cluster(replication_factor=1)
    single_gateway = single.gateway()
    ids = [single_gateway.put(f"archivo-{i}.txt", f"contenido {i}".encode(), {"lote"}) for i in range(20)]
    victim = single_gateway._content.replicas(content_hash(b"contenido 0"))[0]
    show("Archivos guardados", len(ids))
    step(f"Se apaga el almacén {victim}")
    ray.kill(single.storage[victim])
    lost = len(ids) - readable(single_gateway, ids)
    show("Archivos que se pueden leer", f"{len(ids) - lost} de {len(ids)}")
    show("Archivos cuyo contenido se perdió", lost)
    show("Sus fichas siguen existiendo (list)", len(single_gateway.list()))
    print("    → con una sola copia, perder un almacén pierde contenido")

    title("8. Se cae una sección del índice de etiquetas")
    shard = gateway._tag_shard_for("lote")
    show("Antes: archivos con 'lote'", len(gateway.query("lote")))
    show("Antes: proceso de esa sección (PID)", ray.get(shard.pid.remote()))
    step("Se cae el proceso de la sección del índice que guarda 'lote'")
    ray.kill(shard, no_restart=False)  # como un fallo real: Ray la vuelve a arrancar
    show("Después: archivos con 'lote'", len(gateway.query("lote")))
    show("Después: proceso de esa sección (PID)", ray.get(shard.pid.remote()))
    other = cluster.gateway()
    show("Otro cliente: archivos con 'lote'", len(other.query("lote")))
    print("    → Ray la reinició en otro proceso con el mismo handle, y al arrancar")
    print("      se rellenó desde las fichas: sirve igual a todos los clientes")


def client_that_dies_halfway(cluster: RayCluster) -> None:
    gateway = cluster.gateway()
    # Muere justo después de que los metadatos aceptan el archivo y antes de avisar al índice.
    gateway._send_to_index = lambda ops: os._exit(1)
    gateway.put("a-medias.txt", b"guardado a medias", {"a-medias"})


def demo_client_crash(gateway: Gateway, cluster: RayCluster) -> None:
    title("9. Un cliente se cae a mitad de un cambio")
    step("Un cliente guarda un archivo y se cae antes de avisar al índice")
    try:
        run_in_workers(client_that_dies_halfway, [(cluster,)])
    except ray.exceptions.WorkerCrashedError:
        print("    (si Ray avisa de que un worker murió, es esta caída simulada)")
    saved = any(gateway.get(file_id).name == "a-medias.txt" for file_id in gateway.list())
    show("¿Los metadatos tienen el archivo?", saved)
    show("Archivos con 'a-medias' según query", len(gateway.query("a-medias")))
    show("Entradas que faltan en el índice", len(gateway.verify_index().missing))
    step("Otro cliente entrega los cambios pendientes (repair_index)")
    show("Cambios pendientes entregados", gateway.repair_index())
    show("Archivos con 'a-medias' según query", len(gateway.query("a-medias")))
    show("Entradas que faltan en el índice", len(gateway.verify_index().missing))
    print("    → el cambio quedó apuntado en el outbox de su shard de metadatos,")
    print("      y cualquier cliente lo pudo entregar después")


def demo_partition() -> None:
    title("10. Partición de red: un almacén queda aislado y después vuelve")
    cluster = start_cluster()
    gateway = cluster.gateway()
    before = [gateway.put(f"antes-{i}.txt", f"antes {i}".encode(), {"red"}) for i in range(10)]
    isolated = gateway._content.replicas(content_hash(b"antes 0"))[0]
    show("Archivos guardados antes de la partición", len(before))

    step(f"Se aísla {isolated}: sigue vivo y con sus datos, pero nadie puede comunicarse con él")
    ray.get(cluster.storage[isolated].disconnect.remote())
    during = [gateway.put(f"durante-{i}.txt", f"durante {i}".encode(), {"red"}) for i in range(10)]
    gateway.delete(before.pop(0))
    show("Archivos guardados durante la partición", len(during))
    show("Archivo eliminado y basura recolectada", gateway.collect_garbage())
    show("Archivos que se pueden leer", f"{readable(gateway, before + during)} de {len(before + during)}")

    step(f"Vuelve la comunicación con {isolated}")
    ray.get(cluster.storage[isolated].reconnect.remote())
    show("Contenidos fuera de su lugar", misplaced(cluster, gateway))
    show(f"¿{isolated} todavía guarda el contenido borrado?", copies(cluster, b"antes 0") > 0)

    step("Reconciliación")
    report = gateway.reconcile()
    show("Almacenes readmitidos en el anillo", ", ".join(report.rejoined) or "(ninguno)")
    show("Copias escritas donde faltaban", report.copied)
    show("Copias sobrantes borradas", report.removed)
    show("Contenidos borrados por ser basura", report.garbage)
    show("Contenidos fuera de su lugar", misplaced(cluster, gateway))
    show(f"¿{isolated} todavía guarda el contenido borrado?", copies(cluster, b"antes 0") > 0)
    print("    → todo quedó como si la partición no hubiera ocurrido")


def add_tag(cluster: RayCluster, file_id: str, tag: str) -> int:
    # Cada cliente corre en su propio worker de Ray y tiene su propio gateway.
    return cluster.gateway().add_tags(file_id, {tag})


def demo_concurrency(cluster: RayCluster, gateway: Gateway) -> None:
    title(f"11. {CLIENTS} clientes cambian el mismo archivo a la vez")
    file_id = gateway.put("compartido.txt", b"archivo compartido", set())
    remote_add = ray.remote(max_retries=0)(add_tag)
    started = time.time()
    versions = ray.get([remote_add.remote(cluster, file_id, f"cliente-{i}") for i in range(CLIENTS)])
    stored = gateway.get(file_id)
    show("Tiempo", f"{time.time() - started:.1f} s")
    show("Etiquetas finales", f"{len(stored.tags)} de {CLIENTS}")
    show("Versión final", stored.version)
    show("Versiones distintas recibidas", f"{len(set(versions))} (de {min(versions)} a {max(versions)})")
    print("    → ningún cambio se perdió: el shard de metadatos del archivo es un")
    print("      actor y los aplicó uno detrás de otro")


def main() -> None:
    print("Arrancando Ray y el sistema (puede tardar unos segundos)...")
    start_ray(include_dashboard=False, log_to_driver=False, logging_level="ERROR")
    try:
        cluster = start_cluster()
        gateway = cluster.gateway()
        show("Fichas (MetadataShard)", len(cluster.metadata))
        show("Secciones del índice (TagShard)", len(cluster.tags))
        show("Almacenes (StorageNode)", len(cluster.storage))
        demo_operations(cluster, gateway)
        demo_failures(cluster, gateway)
        demo_client_crash(gateway, cluster)
        demo_partition()
        demo_concurrency(cluster, gateway)
        print("\nFin de la demostración.")
    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()
