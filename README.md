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
reconstrucción del índice, un cliente que se cae a mitad de un cambio,
particiones y clientes concurrentes. Las pruebas de
integración arrancan actores reales y pueden tardar varios minutos. Para ejecutar
solo la lógica local: `python -m pytest tests/unit -q`.

## API y ejecución

```python
from tagfs.ray_runtime import start_cluster, stop_cluster

cluster = start_cluster()  # arranca Ray si hace falta y crea los actores
try:
    gateway = cluster.gateway()
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

    # Otro cliente: su propio gateway sobre los mismos actores.
    other = cluster.gateway()
    other_id = other.put("otra.txt", b"otros bytes", {"doc"})
    assert gateway.get(other_id).content == b"otros bytes"
finally:
    stop_cluster()
```

`start_cluster()` es la única forma de arrancar el sistema. Crea 4 shards de
metadatos, 4 de etiquetas y 4 almacenes, con 2 réplicas por contenido; estas
cantidades se configuran con `metadata_shards`, `tag_shards`, `storage_nodes` y
`replication_factor`. El `RayCluster` que devuelve se puede enviar a otros
workers: cada cliente llama a `cluster.gateway()` y obtiene su propio gateway,
que habla directamente con los actores y envía a la vez las peticiones
independientes a varios shards. `python scripts/demo_clients.py` muestra varios
clientes trabajando a la vez.

`get` devuelve un `StoredFile(name, content, tags, version)`, compatible con
acceso por atributos, índices y desempaquetado. Las mutaciones sin efecto
conservan la versión.

Las etiquetas se normalizan a minúsculas. Las consultas admiten `AND`, `OR`,
`NOT` y paréntesis; cada rama necesita una parte positiva que permita obtener
candidatos del índice. Por ejemplo, `foto AND NOT borrador` es válida y
`NOT borrador` se rechaza. Los candidatos se verifican contra los metadatos.

`repair_index()` entrega al índice los cambios que quedaron pendientes y devuelve
cuántos entregó. `verify_index()` compara el índice con los metadatos sin cambiar
nada: devuelve las entradas que faltan (`missing`) y las que sobran (`extra`).

`Gateway(metadata, tags, content)` también acepta shards locales, sin Ray, para
probar la lógica. El comportamiento externo está fijado en
`tests/gateway_contract.py`, que se ejecuta sin Ray y con actores reales.

## Recuperación y límites actuales

- Un shard de etiquetas se reinicia solo. Ray lo vuelve a arrancar con el mismo
  handle cuando muere su proceso, y el gateway pide el reinicio si no responde en
  `index_timeout`. Al arrancar se rellena desde los metadatos, así que la
  reparación sirve a todos los clientes. Se salta los shards de metadatos
  muertos, cuyos archivos ya no están disponibles. Las peticiones pendientes se
  repiten en el actor nuevo, lo que es seguro porque consultar solo lee y aplicar
  un cambio que ya se conoce no tiene efecto.
- Un shard de metadatos no se reinicia: uno nuevo empezaría vacío y diría que
  sus archivos no existen. Si muere, sus archivos dejan de estar disponibles y
  las operaciones que los necesitan fallan.
- Un almacén muerto o aislado sale del anillo. Las escrituras usan los nodos
  disponibles, aunque queden menos réplicas de las solicitadas. `reconcile()`
  readmite nodos que vuelven, repara réplicas faltantes o corruptas, retira copias
  sobrantes y recolecta basura. Su informe incluye contenidos irrecuperables en
  `unavailable`. La reparación se invoca explícitamente.
- `delete()` elimina la ficha lógica y sus entradas del índice. Los bytes se
  conservan hasta `collect_garbage()` o `reconcile()`, pues otros archivos pueden
  compartirlos. Los contadores de basura incluyen solo borrados confirmados.
- La recolección y la reconciliación no deben cruzarse con un `put` de ningún
  cliente: borrarían bytes cuya ficha aún no existe. El llamador debe detener
  las escrituras de todos los clientes durante el mantenimiento.
- No hay gateway central: si un cliente se cae, los demás siguen trabajando.
- Cada cambio queda apuntado en el outbox de su shard de metadatos hasta que el
  índice confirma que lo recibió. Si un cliente se cae después de que los
  metadatos aceptaran el cambio y antes de avisar al índice, o si el índice no
  responde, el cambio ya está hecho y el cliente no recibe error. Las consultas
  no lo ven hasta que algún cliente llame a `repair_index()`, que se puede llamar
  en cualquier momento. Nadie lo llama de forma automática.
- Todo el estado reside en memoria. No hay persistencia en disco ni réplicas de
  los metadatos. Si se pierden todas las copias de un contenido, su ficha sigue
  existiendo, pero la lectura falla.

La persistencia y la replicación de los metadatos, la entrega automática de los
cambios pendientes al índice y la coordinación del mantenimiento entre clientes
quedan pendientes.
