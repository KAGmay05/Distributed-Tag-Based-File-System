# Distributed-Tag-Based-File-System

Sistema de archivos por etiquetas sobre actores de Ray. Los metadatos son la
fuente de verdad; el índice de etiquetas se deriva de ellos. Los bytes se
identifican por SHA-256 y se replican usando un anillo de hashing consistente.

## Instalación y pruebas

Requiere Python 3.10 o posterior y una versión de Python compatible con Ray 2.60.0.
Desde la raíz del repositorio:

```console
python -m pip install -e ".[test]"
python -m pytest -q
python scripts/demo.py
```

La demo recorre operaciones, consultas, deduplicación, fallos de almacenamiento,
reconstrucción del índice, particiones y clientes concurrentes. Las pruebas de
integración arrancan actores reales y pueden tardar varios minutos. Para ejecutar
solo la lógica local: `python -m pytest tests/unit -q`.

## API y ejecución

```python
import ray
from tagfs.gateway import Gateway, start_gateway, start_ray

start_ray(include_dashboard=False)
try:
    gateway = Gateway()
    file_id = gateway.put("nota.txt", b"hola", {"Documento", "trabajo"})
    gateway.add_tags(file_id, {"urgente"})
    stored = gateway.get(file_id)
    assert stored.name == "nota.txt"
    assert stored.content == b"hola"
    assert gateway.query("documento AND NOT borrador") == [file_id]
    gateway.rename(file_id, "nota-final.txt")
    gateway.remove_tags(file_id, {"urgente"})
    gateway.delete(file_id)
    gateway.collect_garbage()

    # Varios clientes comparten este handle y llaman con .remote().
    shared = start_gateway()
    other_id = ray.get(shared.put.remote("otra.txt", b"otros bytes", {"doc"}))
    assert ray.get(shared.get.remote(other_id)).content == b"otros bytes"
finally:
    ray.shutdown()
```

`Gateway()` crea 3 shards de metadatos, 4 de etiquetas y 4 almacenes, con 2
réplicas por contenido. Estas cantidades se configuran con `metadata_shards`,
`tag_shards`, `storage_nodes` y `replication_factor`. `get` devuelve un
`StoredFile(name, content, tags, version)`, compatible con acceso por atributos,
índices y desempaquetado. Las mutaciones sin efecto conservan la versión.

Las etiquetas se normalizan a minúsculas. Las consultas admiten `AND`, `OR`,
`NOT` y paréntesis; cada rama necesita una parte positiva que permita obtener
candidatos del índice. Por ejemplo, `foto AND NOT borrador` es válida y
`NOT borrador` se rechaza. Los candidatos se verifican contra los metadatos.

Se mantiene la API `Gateway(metadata, tags, content)` para conectar objetos
locales o adaptadores síncronos existentes. `start_cluster()` en
`tagfs.ray_runtime` permite distribuir clientes que hablan directamente con los
shards; `python scripts/demo_clients.py` muestra este modo. Cada llamada a
`cluster.gateway()` usa los mismos actores y su propio cliente de contenido.

## Recuperación y límites actuales

- El gateway que crea sus propios actores reconstruye un shard de etiquetas
  muerto o sin respuesta desde los metadatos. Los componentes inyectados y los
  clientes de `RayCluster` conservan su política de errores; no reemplazan
  automáticamente los actores compartidos del índice.
- Un almacén muerto o aislado sale del anillo. Las escrituras usan los nodos
  disponibles, aunque queden menos réplicas de las solicitadas. `reconcile()`
  readmite nodos que vuelven, repara réplicas faltantes o corruptas, retira copias
  sobrantes y recolecta basura. Su informe incluye contenidos irrecuperables en
  `unavailable`. La reparación se invoca explícitamente.
- `delete()` elimina la ficha lógica y sus entradas del índice. Los bytes se
  conservan hasta `collect_garbage()` o `reconcile()`, pues otros archivos pueden
  compartirlos. Los contadores de basura incluyen solo borrados confirmados.
- La recolección y reconciliación deben ejecutarse sin escrituras concurrentes.
  `start_gateway()` serializa todas las operaciones de sus clientes y cumple esa
  condición. Para un objeto `Gateway` usado desde varios hilos, o varios clientes
  independientes del mismo `RayCluster`, el llamador debe detener las escrituras
  de todos ellos durante el mantenimiento.
- Todo el estado reside en memoria. No hay persistencia en disco, replicación de
  metadatos ni recuperación de un shard de metadatos muerto. Si se pierden todas
  las copias de un contenido, su ficha sigue existiendo, pero la lectura falla.
- El gateway compartido es un punto único de fallo; arrancar otro con
  `start_gateway()` crea un sistema vacío. La variante con clientes independientes
  evita ese gateway central, pero aún necesita coordinación de mantenimiento y
  recuperación compartida del índice para ofrecer las mismas garantías.

La persistencia, la recuperación de metadatos y del gateway, y la coordinación
del mantenimiento entre clientes independientes quedan pendientes para ofrecer
durabilidad más allá de esta implementación en memoria.
