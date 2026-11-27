# Distributed-Tag-Based-File-System

Sistema distribuido para guardar archivos y buscarlos por etiquetas con
expresiones booleanas. Los metadatos de cada archivo son la fuente de verdad y
el índice de etiquetas se deriva de ellos. Los bytes se identifican por su
SHA-256 y se replican en varios nodos elegidos con hashing consistente.

Este repositorio es la Fase 1: un prototipo pequeño sobre Ray para entender el
dominio, fijar el contrato externo y averiguar qué mecanismos distribuidos
tendrá que construir la versión nativa. Todo el estado vive en memoria.

## Ejecutar

Requiere Python 3.10 o posterior; `pyproject.toml` fija Ray 2.60.0. Probado con
Python 3.13.7 en una sola máquina con Windows.

```console
python -m venv .venv
.venv\Scripts\activate          # Linux/macOS: source .venv/bin/activate
python -m pip install -e ".[test]"
python -m pytest -q             # suite completa: unos 550 tests, unos 11 minutos
python -m pytest tests/unit -q  # solo la lógica, sin Ray: unos segundos
python scripts/demo.py          # operaciones, fallos, partición y concurrencia
python scripts/demo_clients.py  # varios clientes a la vez y el proceso de cada actor
```

Ray arranca un clúster local: son varios procesos en la misma máquina, no varias
máquinas. Así se obtiene un gateway en este prototipo:

```python
from tagfs.ray_runtime import start_cluster, stop_cluster

cluster = start_cluster()       # arranca Ray si hace falta y crea los actores
gateway = cluster.gateway()     # cada cliente crea el suyo
...
stop_cluster()
```

Si el equipo se suspende, Ray pierde la conexión con sus procesos y la suite
falla. Conviene desactivar la suspensión mientras corre.

## Contrato externo

Es lo que la versión nativa debe conservar. Cómo se arranca el sistema y cómo
obtiene cada cliente su gateway no forma parte del contrato: depende del
despliegue.

Está escrito como pruebas en [tests/gateway_contract.py](tests/gateway_contract.py),
que se ejecutan dos veces: sin Ray, con shards locales
([tests/unit/test_gateway.py](tests/unit/test_gateway.py)), y con actores reales
([tests/integration/test_contract.py](tests/integration/test_contract.py)).

```python
file_id = gateway.put("nota.txt", b"hola", {"Documento", "trabajo"})
gateway.query("documento AND NOT borrador")         # [file_id]
gateway.add_tags_by_query("trabajo", {"revisado"})  # BulkResult(done={file_id: 2}, unknown=[])
gateway.list("revisado")    # [FileInfo(file_id, "nota.txt", {"documento", "trabajo", "revisado"}, 2)]
```

### Operaciones

| Operación | Resultado |
| --- | --- |
| `put(name, content, tags=())` | Crea un archivo y devuelve su ID. `name` debe ser texto y `content`, `bytes` |
| `get(file_id)` | Devuelve `StoredFile(name, content, tags, version)` |
| `add_tags(file_id, tags)` | Agrega etiquetas y devuelve la versión resultante |
| `remove_tags(file_id, tags)` | Quita etiquetas y devuelve la versión resultante |
| `rename(file_id, new_name)` | Cambia el nombre y devuelve la versión resultante |
| `delete(file_id)` | Elimina el archivo |
| `query(expr)` | Devuelve, ordenados, los IDs de los archivos que cumplen la consulta |
| `list(expr=None)` | Devuelve, ordenados por ID, un `FileInfo(file_id, name, tags, version)` por cada archivo que cumple la consulta, sin leer su contenido. Sin consulta, uno por cada archivo vivo |
| `delete_by_query(expr)` | Elimina los archivos que cumplen la consulta y devuelve un `BulkResult` |
| `add_tags_by_query(expr, tags)` | Agrega etiquetas a los archivos que cumplen la consulta y devuelve un `BulkResult` |
| `remove_tags_by_query(expr, tags)` | Quita etiquetas a los archivos que cumplen la consulta y devuelve un `BulkResult` |

`StoredFile`, `FileInfo` y `BulkResult` también se pueden usar como tuplas.
`BulkResult(done, unknown)` tiene dos partes:

- `done` relaciona cada archivo que cumplía la consulta con su versión resultante.
  Incluye los que ya estaban como se pedía, con su versión sin cambiar, igual que
  `add_tags` sobre un solo archivo.
- `unknown` lista los posibles archivos de los shards de metadatos que no
  respondieron. De ellos no se sabe si cambiaron.

El tema pide `add`, `delete`, `list`, `add-tags` y `delete-tags` sobre consultas
de etiquetas. Así se corresponden:

| Del tema | Por consulta | Sobre un archivo |
| --- | --- | --- |
| `add` | No aplica: crea un archivo nuevo | `put` |
| `delete` | `delete_by_query` | `delete` |
| `list` | `list(expr)`, o `query(expr)` si bastan los IDs | `get` |
| `add-tags` | `add_tags_by_query` | `add_tags` |
| `delete-tags` | `remove_tags_by_query` | `remove_tags` |

Las operaciones por consulta son métodos aparte, y no una variante de las de un
archivo, porque un ID también es una etiqueta y una consulta válidas: `delete(x)`
no podría saber si `x` es un archivo o una consulta, y una confusión así borra
archivos.

### Reglas

- Cada `put` genera un ID nuevo, aunque el nombre y los bytes se repitan. Dos
  archivos pueden compartir los mismos bytes y tener metadatos independientes.
  Un ID nunca se reutiliza.
- La versión empieza en 1 y sube con cada cambio efectivo. Agregar una etiqueta
  que ya está, quitar una que no está o repetir el nombre no la cambia.
- Las etiquetas se guardan en minúsculas. Se rechazan las vacías, las que no son
  texto, las que tienen espacios o paréntesis y las palabras `and`, `or` y `not`.
  Un texto suelto no es un conjunto de etiquetas: `add_tags(f, "foto")` se rechaza.
- Las consultas admiten `AND`, `OR`, `NOT` y paréntesis, y cada rama necesita una
  parte positiva: `foto AND NOT borrador` vale, `NOT borrador` no.
- Una petición mal formada no cambia nada: se valida entera antes de guardar.
- Repetir `delete` sobre un archivo eliminado no cambia nada. Eliminar no borra
  los bytes al momento, porque otro archivo puede usarlos.
- Un cambio queda hecho cuando lo aceptan los metadatos. Si después el índice no
  responde, el cliente no recibe error y las consultas no ven el cambio hasta que
  llega al índice (ver [garantías](#garantías-y-mantenimiento)).

Reglas de las operaciones por consulta:

- Se decide archivo por archivo, en el momento de cambiarlo: un archivo que dejó
  de cumplir la consulta mientras tanto no se toca. No es una foto de todos los
  archivos en un mismo instante.
- Igual que `query`, solo alcanzan a los archivos que el índice ya conoce: un
  cambio aceptado que aún no llegó al índice puede quedar fuera.
- Si un shard de metadatos no responde, lo que hicieron los demás no se deshace:
  sus archivos salen en `done`, y los del que falló, en `unknown`.
- Repetir la operación no repite los cambios: los archivos que ya no cumplen la
  consulta no se tocan, y los que la siguen cumpliendo ya están como se pedía.
  Por eso un segundo `delete_by_query` devuelve `done` vacío.

### Errores que ve el cliente

| Error | Cuándo |
| --- | --- |
| `FileNotFound` | El archivo no existe, fue eliminado o el ID nunca existió |
| `InvalidTag`, `InvalidQuery` | La etiqueta o la consulta están mal formadas; no se guarda nada |
| `ContentNotFound` | La ficha existe, pero ninguna réplica alcanzable tiene los bytes |
| `ContentCorrupted` | Los bytes no coinciden con su hash |
| `StorageUnavailable` | Ningún almacén pudo guardar el contenido de un `put` |
| `ServiceUnavailable` | Un shard de metadatos que la operación necesita murió o no respondió en `metadata_timeout` (10 s), o un shard de etiquetas no respondió ni después de reiniciarlo. Si ocurre durante un cambio, el cambio puede haberse hecho o no. En una operación por consulta solo aparece si falla la búsqueda de candidatos en el índice, y entonces no se cambió nada |

Todos son subclases de `TagFSError` y ninguno es un tipo de Ray: el gateway
traduce los errores de Ray antes de que lleguen al cliente, así que el contrato
no cambia al quitar Ray. Aparte, un nombre que no es texto o un contenido que no
es `bytes` lanzan `TypeError`, como cualquier función de Python que recibe un
argumento del tipo equivocado.

### Garantías y mantenimiento

Se congelan las garantías, no los métodos que hoy las dan. El prototipo tiene
cuatro operaciones de mantenimiento que hay que llamar a mano; la versión nativa
puede dar las mismas garantías sin ellas, por ejemplo entregando lo pendiente en
segundo plano.

| Garantía (se congela) | Hoy la da (no se congela) |
| --- | --- |
| Un cambio aceptado acaba viéndose en las consultas | La entrega al índice dentro de la misma operación y, si el índice no respondió, `repair_index()`, que entrega los cambios pendientes y dice cuántos entregó |
| El índice acaba igual que los metadatos | Lo mismo. `verify_index()` lo comprueba sin cambiar nada y devuelve lo que falta (`missing`) y lo que sobra (`extra`) |
| El contenido que ningún archivo vivo usa acaba borrándose | `collect_garbage()`, que dice cuántos contenidos borró |
| Tras un fallo o una partición, cada contenido vuelve a tener sus copias donde le toca | `reconcile()`, que devuelve un informe |

Estas garantías se prueban aparte, en
[tests/maintenance_checks.py](tests/maintenance_checks.py), también con y sin Ray.

Lo que el contrato todavía no promete, porque el prototipo no lo cumple:

- Reintentar un `put` tras perder la respuesta crea otro archivo, con otro ID.
- Si se pierde la respuesta de un cambio y el índice tampoco lo recibió,
  reintentarlo devuelve éxito, porque el cambio ya está hecho, pero las consultas
  no lo ven hasta que llega al índice.

## Arquitectura

No hay un servidor central. Cada cliente tiene su propio `Gateway`, que corre en
el proceso del cliente y habla directamente con los actores.

| Componente | Dónde corre | Responsabilidad | Estado que guarda |
| --- | --- | --- | --- |
| `Gateway` | En cada cliente | Coordina las operaciones de ese cliente | Handles de los actores y timeouts |
| `ContentStore` | En cada cliente, dentro de su gateway | Elige réplicas, verifica lecturas, repara y recolecta | Su propia vista de qué almacenes están vivos |
| `MetadataShard` | Actor | Guarda la ficha autoritativa de cada archivo | Fichas y outbox de cambios pendientes para el índice |
| `TagShard` | Actor que Ray reinicia | Relaciona etiquetas con archivos | Índice derivado, con la versión de cada entrada |
| `StorageNode` | Actor | Guarda los bytes por su hash | Contenido |

```mermaid
flowchart TD
    C1[Cliente 1: Gateway + ContentStore] --> M[4 MetadataShard]
    C1 --> T[4 TagShard]
    C1 --> N[4 StorageNode]
    C2[Cliente 2: Gateway + ContentStore] --> M & T & N
    T -. al reiniciarse lee .-> M
```

`start_cluster()` es la única forma de arrancar el sistema. Crea los actores una
vez y devuelve un `RayCluster` que se puede enviar a otros procesos; cada cliente
llama a `cluster.gateway()`. Por defecto hay 4 shards de metadatos, 4 de
etiquetas, 4 almacenes y factor de replicación 2. Los metadatos se reparten por
ID de archivo y el índice por etiqueta con [router.py](src/tagfs/router.py); el
contenido, con el anillo de [ring.py](src/tagfs/ring.py). `Gateway(metadata,
tags, content)` también acepta shards locales sin Ray, para probar la lógica.

Una escritura sigue estos pasos:

1. El gateway valida las etiquetas y genera el ID.
2. `ContentStore` calcula el SHA-256 y envía los bytes a las réplicas que indica
   el anillo. Si una falla, prueba con el siguiente nodo.
3. El shard de metadatos crea la ficha, apunta los cambios de índice (`IndexOp`)
   en su outbox y los devuelve.
4. El gateway envía los `IndexOp` a los shards de etiquetas. Si uno no responde
   en `index_timeout`, pide a Ray que lo reinicie y vuelve a esperar.
5. Si el índice confirmó, el gateway saca el cambio del outbox. Si no, el cambio
   se queda ahí para `repair_index()`.

Una consulta pide a la vez al índice los archivos de cada etiqueta positiva, lee
las fichas de los candidatos y se queda con los que cumplen la expresión según
sus etiquetas actuales. Así se descartan los falsos positivos de un índice
atrasado, pero no se recuperan los archivos que le falten.

Un cambio por consulta empieza igual, pidiendo los candidatos al índice. Después
envía a cada shard de metadatos, a la vez, una sola llamada con sus candidatos y
la consulta (`delete_matching`, `add_tags_matching` o `remove_tags_matching`). El
shard vuelve a evaluar la consulta con las etiquetas actuales de cada archivo y
lo cambia en esa misma llamada, así que nadie puede cambiar el archivo entre la
comprobación y el cambio. Al final, el gateway entrega al índice todos los
cambios juntos, con un solo `apply_many` por shard de etiquetas.

### Procesos y piezas de Ray

Los componentes de arriba viven en procesos que crea y vigila Ray. Estas son
las piezas de Ray que intervienen:

| Pieza de Ray | Qué es | Qué hace en este sistema |
| --- | --- | --- |
| Driver | El proceso que llama a `start_cluster()`: el script de la demo o pytest | Es dueño de los actores: si termina, Ray los destruye todos. Guarda el `RayCluster` y puede ser también un cliente |
| Workers | Procesos de Python que arranca Ray | Uno por cada uno de los 12 actores, otro por cada cliente de `run_in_workers()` y otro para `_StartBarrier` |
| GCS | El registro central del clúster, en el nodo principal | Apunta cada actor y su nombre, detecta cuándo muere y ordena reiniciarlo. Atiende `ray.get_actor()` y `ray.kill()` |
| Raylet | Un proceso por máquina | Arranca los workers que pide el GCS y les asigna CPU; sin CPU libre, un actor no se coloca |
| Object store | Memoria compartida de cada máquina, gestionada por su raylet | Guarda los bytes del `ray.put` de `ContentStore` hasta que los leen las réplicas, y los resultados grandes |

```mermaid
flowchart TB
    subgraph maquina[Máquina: un nodo de Ray]
        subgraph driver[Proceso driver]
            SC[start_cluster + RayCluster]
            GD[Gateway + ContentStore del driver]
        end
        subgraph clientes[Workers de clientes]
            GC[Gateway + ContentStore de cada cliente]
        end
        subgraph actores[12 workers de actores]
            MS[4 MetadataShard]
            TS[4 TagShard]
            SN[4 StorageNode]
        end
        RL[Raylet]
        OS[(Object store)]
    end
    GCS[GCS: actores, nombres, muertes y reinicios]

    SC -- crea actores --> GCS
    GCS -- pide procesos --> RL
    RL -- arranca --> actores
    RL -- arranca --> clientes
    TS -- busca shards por nombre al arrancar --> GCS
    GD & GC -- llamadas a métodos --> MS & TS & SN
    GD & GC -- ray.put de los bytes --> OS
    OS -- los lee --> SN
    GD & GC -- ray.kill de un TagShard colgado --> GCS
```

En una ejecución local todo está en la misma máquina: un raylet, un object store
y el GCS. Con varias máquinas, cada una tendría su raylet y su object store, y el
GCS seguiría siendo uno solo.

De cada componente nuestro, en qué proceso vive y de qué pieza de Ray depende:

| Componente | Proceso | Depende de | Para qué |
| --- | --- | --- | --- |
| `start_cluster()` y `RayCluster` | Driver | GCS y raylet | Registrar los actores, darles nombre y esperar a que arranquen |
| `Gateway` | El del cliente: el driver o un worker de cliente | Workers de los actores | Llamar a sus métodos con `.remote()` y esperar con `ray.get` |
| `ContentStore` | El del cliente, dentro de su gateway | Object store | Enviar los bytes una sola vez aunque vayan a varias réplicas |
| `MetadataShard` | Su propio worker, uno por shard | Raylet | Que exista el proceso; si muere, nadie lo vuelve a arrancar |
| `TagShard` | Su propio worker, uno por shard | GCS | Reiniciarlo con el mismo handle y encontrar los shards de metadatos por nombre |
| `StorageNode` | Su propio worker, uno por almacén | Object store | Recibir los bytes del `ray.put` |
| Clientes de `run_in_workers()` | Un worker por cliente | Raylet | Arrancarlos a la vez; declaran `num_cpus=0` para no esperar CPU |
| `_StartBarrier` | Su propio worker | Raylet | Solo para pruebas y demos: que los clientes empiecen juntos |

Lo que hoy da por hecho el código y no da ningún componente nuestro: que los
actores tengan nombre (GCS), que un `TagShard` muerto vuelva (GCS) y que haya un
proceso para cada actor (raylet). La versión nativa tendrá que dar cada una de
estas cosas por su cuenta.

## Qué hace Ray en este prototipo

### Tasks y actores

Los actores son los 12 shards y almacenes que crea `start_cluster()`. `Gateway`
y `ContentStore` no son actores: son objetos normales dentro de cada cliente.

Las tasks son los clientes de las pruebas y de las demos. Cada una recibe el
`RayCluster` y crea su propio gateway. `run_in_workers()` los lanza y usa un
actor auxiliar, `_StartBarrier`, para que todos empiecen a la vez; solo existe
para las pruebas y las demos.

### Qué va en paralelo y qué en serie

Los clientes corren en procesos distintos, así que sus operaciones ocurren al
mismo tiempo de verdad y solo hacen cola en el actor que comparten. Dentro de
una operación también van en paralelo, porque se envían todas las peticiones
antes de esperar:

- la escritura de un contenido en sus réplicas;
- la lectura de varias réplicas, quedándose con la primera válida (`ray.wait`);
- la entrega de los `IndexOp` a los shards de etiquetas;
- las búsquedas de todas las etiquetas de una consulta;
- la lectura de las fichas de los candidatos, una llamada por shard;
- los cambios por consulta, una llamada por shard de metadatos;
- `list()`, `repair_index()` y `verify_index()`, que preguntan a todos los shards.

[test_parallel_requests.py](tests/integration/test_parallel_requests.py) lo
comprueba sin medir tiempos. Usa shards que no responden hasta recibir todas las
peticiones esperadas: si el gateway esperara cada respuesta antes de enviar la
siguiente, la prueba fallaría por timeout.

En serie van los pasos de una escritura (contenido, metadatos, índice y
confirmación), a propósito: los metadatos no deben apuntar a bytes que aún no
existen. Y cada actor atiende una llamada a la vez.

Esa serialización de los actores sostiene invariantes que la versión nativa
tendrá que garantizar por su cuenta:

| Invariante | Qué la garantiza hoy | Qué hará falta sin Ray |
| --- | --- | --- |
| Dos cambios sobre un archivo no se pisan y cada uno recibe su versión | El `MetadataShard` atiende una llamada a la vez | Exclusión mutua por shard o por archivo |
| El índice converge aunque los `IndexOp` lleguen desordenados, repetidos o de clientes distintos | La versión de cada `IndexOp`; cada `apply` es atómico porque el actor es secuencial | Un lock alrededor de `apply` |
| Un cambio pendiente no devuelve al índice un estado viejo | `pending()` calcula el estado actual de cada etiqueta dentro del actor | Leer y responder bajo el mismo lock |
| Un cambio por consulta no toca un archivo que dejó de cumplirla | El `MetadataShard` comprueba la consulta y cambia el archivo en la misma llamada | Comprobar y cambiar bajo el mismo lock |
| La recolección no borra los bytes de un `put` que aún no tiene ficha | Nada: quien la llama debe detener antes las escrituras de todos los clientes | Coordinar explícitamente `put` y recolección |

El experimento de 50 clientes sale bien porque el shard de metadatos del archivo
aplica los cambios uno detrás de otro, no porque la aplicación controle la
concurrencia.

### Datos

El único `ray.put` explícito está en `ContentStore`: los bytes se serializan una
vez aunque se envíen a varias réplicas. Como `ContentStore` corre en el cliente,
los bytes van del cliente al object store de Ray y de ahí a cada réplica, y en
una lectura vuelven del almacén al cliente, sin pasar por ningún intermediario.

Lo demás viaja como argumento o resultado de un método: nombres, etiquetas,
`FileMeta`, `Mutation`, `IndexOp`, conjuntos de IDs. Cada cliente espera sus
`ObjectRef` dentro de la misma operación; ninguno se guarda para después. Dos
datos más se mueven entre procesos: el `RayCluster`, con los handles de todos
los actores, viaja a cada cliente; y cada `TagShard` recibe los nombres de los
shards de metadatos, no sus handles, para poder volver a buscarlos con
`ray.get_actor()` cuando Ray lo reinicia.

| Estado | Disponible | Reconstruible | Durable |
| --- | --- | --- | --- |
| Metadatos y outbox | Mientras viva su shard | No: son la fuente de verdad y no tienen copia | No |
| Índice | Mientras viva su shard, y Ray lo reinicia | Sí: al reiniciarse se rellena desde los metadatos | No |
| Contenido | Mientras quede una réplica alcanzable | No: los bytes vienen del cliente y no se pueden recalcular | No |
| Vista de almacenes vivos | Mientras viva el cliente | Sí: cada gateway empieza con todos y `reconcile()` readmite los que vuelven | No |

### Recursos y placement

Los actores no declaran recursos ni estrategias de placement. En Ray 2.60, un
actor así necesita una CPU libre para colocarse, pero no la reserva mientras
vive. Los clientes de `run_in_workers()` declaran `num_cpus=0`, así que todos
pueden correr a la vez aunque haya pocas CPU; los de la sección 11 de la demo no
declaran nada y reservan una CPU cada uno, de modo que cuántos corren a la vez
depende de la máquina. Las pruebas arrancan Ray con 2 CPU.

Sin CPU, los actores no se pueden colocar y se quedan esperando. Se comprobó con
`start_ray(num_cpus=0)`: `start_cluster()` falla con `GetTimeoutError` al cumplirse
`WORKER_START_TIMEOUT_SECONDS` (120 s).

El placement no afecta a la corrección. Todos los accesos van por handles o por
nombre, y qué shard o almacén guarda cada clave lo decide la aplicación, no Ray.
Ray solo elige en qué proceso y en qué máquina corre cada actor. Sí afecta a la
garantía de replicación: nada impide que las dos réplicas de un contenido acaben
en la misma máquina, y en una ejecución local siempre lo están. El factor 2
protege contra la pérdida de un proceso, no de una máquina.

### Fallos y reintentos

| Proceso | Configuración | Qué pasa si muere |
| --- | --- | --- |
| Cliente (task) | `max_retries=0`; por defecto Ray la repetiría hasta 3 veces | Ray no la repite y quien la espera recibe `WorkerCrashedError`. Los demás clientes siguen |
| `TagShard` | `max_restarts=-1`, `max_task_retries=-1` | Ray lo arranca en otro proceso con el mismo handle, lo rellena desde los metadatos y repite las llamadas que estaban en curso |
| `MetadataShard` | Valores por defecto: ni reinicio ni reintentos | Sus archivos quedan inaccesibles y quien los pide recibe `ServiceUnavailable`. Es a propósito: un shard nuevo empezaría vacío y diría que sus archivos no existen |
| `StorageNode` | Valores por defecto | El `ContentStore` de cada cliente lo saca de su anillo al detectarlo; `reconcile()` lo readmite si vuelve |
| Proceso que llamó a `start_cluster()` | Los actores no son *detached* | Ray destruye todos los actores, porque pertenecen a ese proceso |

Además, si un `TagShard` no responde en `index_timeout` (5 s), el gateway lo mata
con `ray.kill(no_restart=False)` para que Ray lo reinicie. Un shard lento se trata
como uno muerto. Hay un detalle de Ray que condiciona el código: Ray no reinicia
un actor cuyo `__init__` falló, así que el `TagShard` se salta al arrancar los
shards de metadatos muertos; si no, perdería también las etiquetas de los vivos.

¿Es seguro repetir cada operación?

| Operación | Repetirla |
| --- | --- |
| `put` | No es seguro: cada repetición crea otro archivo con otro ID |
| `get`, `query`, `list`, `verify_index` | Seguro: solo leen |
| `add_tags`, `remove_tags`, `rename` | Seguro si nadie más toca el archivo entre medias; si alguien lo cambió, la repetición puede deshacer su cambio |
| `delete` | Seguro: la segunda vez no cambia nada |
| `delete_by_query`, `add_tags_by_query`, `remove_tags_by_query` | No repite cambios: lo ya cambiado no vuelve a cambiar. Pero se aplica a lo que cumpla la consulta en ese momento, así que también alcanza a un archivo que empezó a cumplirla entre los dos intentos |
| `repair_index` | Seguro: entregar dos veces un cambio no tiene efecto |
| `collect_garbage`, `reconcile` | Seguro, si no se cruzan con un `put` |
| `TagShard.apply_many`, `TagShard.lookup` | Seguro: la versión descarta duplicados, aunque se repita solo parte del lote, y `lookup` solo lee |
| `StorageNode.put` | Seguro: la clave es el hash del contenido |

Esta tabla explica la configuración: el `TagShard` puede dejar que Ray repita sus
llamadas, y los clientes no, porque un `put` repetido crea un duplicado. Desactivar
el reintento de Ray no lo resuelve del todo: un cliente que reintenta por su
cuenta tras perder la respuesta también crea otro archivo.

Límites conocidos:

- No hay persistencia ni réplicas de los metadatos.
- Un shard de metadatos colgado, pero vivo, no se reinicia: cada operación que
  lo necesite falla con `ServiceUnavailable` tras `metadata_timeout` (10 s).
  `list()`, las consultas, la recolección y `repair_index()` los necesitan todos.
- Contenido y metadatos no se escriben de forma atómica: una caída entre los dos
  pasos deja bytes huérfanos hasta la siguiente recolección.
- `repair_index()` y `reconcile()` hay que llamarlos a mano.
- Una escritura se acepta en cuanto una réplica confirma, aunque falten otras.
- La recolección y la reconciliación no deben cruzarse con un `put` de ningún
  cliente, y nada lo impide.
- Cada cliente tiene su propia vista de qué almacenes están vivos. Dos clientes
  pueden no coincidir y colocar copias en sitios distintos hasta que alguien
  llame a `reconcile()`.
- Un contenido inaccesible durante un aislamiento vuelve a estar disponible
  cuando el almacén reaparece.

### Observabilidad

Lo que se puede observar hoy:

- Qué proceso ejecuta cada actor. Todos responden a `pid()`.
  [test_ray_cluster.py](tests/integration/test_ray_cluster.py) comprueba que los
  12 actores corren en 12 procesos distintos del script, y
  [demo_clients.py](scripts/demo_clients.py) imprime el PID de cada actor y de
  cada cliente.
- Un reinicio. La sección 8 de la demo muestra el PID del `TagShard` antes y
  después de matarlo, y el log de Ray avisa de que la llamada en curso falló y
  se repetirá en el actor nuevo.
- Que un cliente no se repite. Una prueba de `test_ray_cluster.py` hace que un
  cliente incremente un contador y mate su propio proceso; el contador queda en
  1, así que Ray no lo ejecutó otra vez.

Lo que falta: relacionar cada petición con su task, su intento, su proceso y su
nodo. Hoy no se puede saber cuántas veces repitió Ray una llamada concreta al
`TagShard` reiniciado ni qué petición del cliente correspondía. El siguiente paso
sería que cada operación lleve un `request_id` y que cliente y actores lo
registren junto con el ID de la task (`ray.get_runtime_context()`), el PID y el
nodo.

## Experimentos y resultados

Resultados de una ejecución de [scripts/demo.py](scripts/demo.py) y
[scripts/demo_clients.py](scripts/demo_clients.py) en una sola máquina. Las
pruebas de [fallos del índice](tests/integration/test_gateway_index_failures.py),
[particiones](tests/integration/test_partitions.py) y
[extremo a extremo](tests/integration/test_end_to_end.py) comprueban los mismos
escenarios con aserciones.

| Experimento | Qué se hace | Resultado observado |
| --- | --- | --- |
| Deduplicación | Dos archivos con los mismos bytes | IDs distintos y 2 copias del contenido en total. Al borrar uno, la recolección no borra nada; al borrar el otro, borra el contenido |
| Cae un almacén, factor 2 | `ray.kill()` a un `StorageNode` | 20 de 20 archivos legibles; un archivo nuevo se guarda con 2 copias |
| Cae un almacén, factor 1 | Lo mismo con una sola copia | 14 de 20 legibles; las 20 fichas siguen existiendo |
| Cae un shard del índice | `ray.kill(no_restart=False)` al `TagShard` de una etiqueta | Ray lo reinicia en otro proceso (PID 13084 → 32408); la consulta devuelve 21 archivos antes y después, también desde otro cliente |
| Un cliente cae a mitad de un cambio | El cliente muere después de que los metadatos aceptan un `put` y antes de avisar al índice | El archivo existe, pero la consulta no lo encuentra y falta 1 entrada en el índice. `repair_index()` desde otro cliente entrega 1 cambio y la consulta lo encuentra |
| Partición y vuelta | `disconnect()`, escrituras y un borrado, `reconnect()` y `reconcile()` | 19 de 19 legibles durante el aislamiento; al volver, 3 contenidos fuera de lugar; tras reconciliar, 5 copias escritas, 5 sobrantes y 1 contenido basura borrados, y 0 fuera de lugar |
| 50 clientes sobre un archivo | 50 tasks agregan una etiqueta distinta al mismo archivo | 50 etiquetas, versión final 51 y 50 versiones distintas, de 2 a 51 |
| 4 clientes en paralelo | Cada uno crea 25 archivos | 17 procesos distintos (script, 12 actores, 4 clientes); 100 archivos en 0,59 s, con los 4 activos a la vez durante 0,34 s; las consultas coinciden con lo calculado desde los metadatos |

`disconnect()` hace que el almacén rechace peticiones; no toca la red del sistema
operativo. Matar un actor simula la pérdida de un proceso, no de una máquina. Los
tiempos incluyen el arranque de tasks y el scheduling de Ray; no son una medida
de rendimiento.

### Experimento: un TagShard colgado

Se guardan archivos con una etiqueta, se cuelga el `TagShard` de esa etiqueta y
se consulta hasta obtener la respuesta correcta. El shard se cuelga con una
llamada que duerme una hora, enviada con `max_task_retries=0`. Sin esa opción,
Ray repetiría también el cuelgue en el actor nuevo y no se recuperaría nunca. Se
ejecuta 3 veces sin fallo (control) y 3 con fallo, con la configuración por
defecto: 4 shards de cada tipo, factor 2, `index_timeout` de 5 s.

Predicción, escrita antes de ejecutar nada:

| Medida | Control | Con fallo | Por qué |
| --- | --- | --- | --- |
| Tiempo hasta la primera consulta correcta | Menos de 0,1 s | Entre 7 y 10 s | El gateway espera `index_timeout` (5 s), mata el shard para que Ray lo reinicie y espera al actor nuevo: arrancar un proceso de Python, importar `tagfs` y leer los 4 shards de metadatos |
| Archivos perdidos | 0 | 0 | El actor nuevo rellena el índice desde los metadatos, que no se tocan |
| PID del shard | No cambia | Cambia | Ray arranca el actor en otro proceso |
| `verify_index()` al terminar | Sin diferencias | Sin diferencias | El índice reconstruido es exactamente el de los metadatos |
| Consulta a otro shard durante el cuelgue | Menos de 0,1 s | Igual que en el control | El gateway no pasa por el shard colgado |

Se ejecutó con [scripts/experiment_index_hang.py](scripts/experiment_index_hang.py)
en una sola máquina (Windows 11, Python 3.13, Ray 2.60). Cada ejecución guarda un
JSON en [experiments/results/](experiments/results/), con la configuración, cada
intento de consulta y el PID antes y después.

| Medida | Predicho con fallo | Observado: control (3) | Observado: con fallo (3) | ¿Acertó? |
| --- | --- | --- | --- | --- |
| Tiempo hasta la primera consulta correcta | 7 a 10 s | 0,005 a 0,006 s | 8,91, 8,96 y 9,02 s | Sí, en la parte alta del rango: el reinicio costó unos 4 s además de los 5 s de timeout |
| Archivos perdidos | 0 de 20 | 0 de 20 | 0 de 20 | Sí |
| PID del shard | Cambia | No cambia | Cambia en las 3 | Sí |
| `verify_index()` al terminar | Sin diferencias | 0 faltan, 0 sobran | 0 faltan, 0 sobran | Sí |
| Consulta a otro shard durante el cuelgue | Igual que el control | 0,006 a 0,007 s | 0,005 a 0,007 s | Sí |

En qué se quedó corta la predicción:

- No decía qué ve el cliente. No recibe ningún error ni tiene que reintentar: la
  primera consulta tarda unos 9 s y ya devuelve los 20 archivos. Todo el
  reinicio ocurre dentro de esa llamada.
- No preveía la única huella del reintento: el raylet escribe en el log que
  `lookup` falló porque el actor se reinició y que lo repetirá. El gateway no se
  entera de que su petición se ejecutó dos veces.
- No separaba las partes del tiempo. Los 5 s del timeout son fijos; los otros
  4 s, que incluyen arrancar el proceso y rellenar el índice, no se midieron por
  separado. Con muchos más archivos, rellenar el índice tardaría más.

## Retirada de Ray

Mapa de lo que el sistema necesita y hoy le da Ray, ordenado por requisito. Cada
fila dice qué lo da hoy, qué prueba o experimento lo demuestra y qué hay que
decidir para la versión nativa.

| Requisito | Qué lo da hoy | Evidencia | Pregunta para la versión nativa |
| --- | --- | --- | --- |
| **Ejecución remota**: llamar a un método de otro proceso, enviar varias llamadas antes de esperar, poner plazo a cada espera y recibir el resultado o un error del contrato | `.remote()`, `ray.get(timeout=...)` y `ray.wait()`. Ray serializa argumentos y resultados, y envuelve las excepciones en `RayTaskError`; el gateway las desenvuelve y convierte las de Ray en `ServiceUnavailable`. `runtime_env` copia el paquete `tagfs` a cada worker | [test_parallel_requests.py](tests/integration/test_parallel_requests.py): las peticiones a varios shards salen a la vez. [test_gateway_errors.py](tests/integration/test_gateway_errors.py): `FileNotFound` llega sin envoltorio y un shard colgado da `ServiceUnavailable` en menos de 6 s | ¿Basta gRPC con un plazo por llamada y códigos propios para `FileNotFound`, `InvalidTag` y `ServiceUnavailable`? ¿Cómo se envían varias peticiones a la vez y se espera la primera réplica válida sin `ray.wait`? |
| **Estado en un worker**: 12 dueños fijos de partes del estado, cada uno en su proceso y atendiendo una llamada a la vez | Actores de Ray: el estado vive en la memoria del proceso y cada actor tiene una cola de llamadas. Los actores pertenecen al driver y mueren con él. Nada persiste ni replica esa memoria | [test_ray_cluster.py](tests/integration/test_ray_cluster.py) `test_each_actor_runs_in_its_own_process`: 12 actores en 12 procesos. [test_gateway_clients.py](tests/integration/test_gateway_clients.py) y el experimento de 50 clientes: ningún cambio concurrente se pierde, versión final 51. [test_end_to_end.py](tests/integration/test_end_to_end.py) `test_metadata_shard_failure_loses_its_files`: un shard de metadatos muerto pierde sus archivos | ¿Exclusión mutua por shard o por archivo, si los handlers atienden en paralelo? ¿Un log por shard con replay al arrancar, para que los metadatos sobrevivan a un reinicio? ¿Servicios con su propio ciclo de vida, que no dependan de un proceso dueño? |
| **Scheduling**: colocar cada servicio en un proceso y una máquina, y separar las réplicas de un contenido en máquinas distintas | El scheduler de Ray y el raylet eligen dónde corre cada actor. Qué shard o almacén guarda cada clave lo decide la aplicación con [router.py](src/tagfs/router.py) y [ring.py](src/tagfs/ring.py) | [test_router.py](tests/unit/test_router.py) `test_values_are_pinned_so_every_process_agrees`: el reparto no depende de Ray, cada proceso calcula el mismo shard para cada clave. En local las dos réplicas están siempre en la misma máquina (sección "Recursos y placement") | ¿Asignación estática por configuración, con la máquina de cada nodo? ¿Cómo se garantiza que el anillo no ponga dos réplicas en la misma máquina? |
| **Resultado remoto**: llevar los bytes de un contenido a N réplicas sin serializarlos N veces | `ray.put()` deja los bytes en el object store una vez y cada réplica recibe el `ObjectRef`. Ningún `ObjectRef` se guarda después de su operación | [content_store.py](src/tagfs/content_store.py): el único `ray.put` explícito. [test_content_store.py](tests/integration/test_content_store.py) `test_put_stores_exactly_r_copies_on_the_nodes_of_the_ring` | ¿Basta con enviar los bytes en cada petición, aceptando N copias en la red? ¿Hace falta streaming para archivos grandes? No hace falta un almacén de resultados ni recomputación por linaje: el contenido lo aporta el cliente |
| **Membresía**: que clientes y shards encuentren los 12 servicios y sepan cuáles están vivos | El `RayCluster` lleva los handles a cada cliente. El GCS guarda los nombres de los shards de metadatos, que el `TagShard` busca con `ray.get_actor()` al arrancar. Cada `ContentStore` tiene su propia vista de almacenes vivos | [test_gateway_index_failures.py](tests/integration/test_gateway_index_failures.py) `test_a_tag_shard_restarts_even_after_a_metadata_shard_died`: el `TagShard` se salta los shards cuyo nombre ya no existe. [test_partitions.py](tests/integration/test_partitions.py) `test_a_partitioned_node_keeps_its_data_and_rejoins`: `reconcile()` readmite un almacén | ¿Basta una configuración estática con las direcciones, si el conjunto no cambia? ¿Basta que cada cliente tenga su vista de vivos, o hace falta una membresía compartida? |
| **Reintentos**: repetir solo lo que es seguro repetir, relanzar un shard de etiquetas con la misma identidad y no repetir nunca un `put` | En el `TagShard`, `max_restarts=-1` y `max_task_retries=-1`; el gateway lo mata con `ray.kill(no_restart=False)` si no responde en `index_timeout`. En los clientes, `max_retries=0` | [Experimento del TagShard colgado](#experimento-un-tagshard-colgado): se recupera en unos 9 s, sin perder archivos y con otro PID. [test_gateway_index_failures.py](tests/integration/test_gateway_index_failures.py) `test_a_tag_shard_that_stops_answering_is_restarted`. [test_ray_cluster.py](tests/integration/test_ray_cluster.py) `test_a_client_whose_worker_dies_is_not_run_again`: el contador queda en 1 | ¿Quién relanza un servicio caído en la misma dirección, un supervisor? ¿Lo reintenta el cliente con timeout, sabiendo que `lookup` y `apply` son idempotentes? ¿Un `request_id` y deduplicación en los metadatos, para que reintentar un `put` no cree otro archivo? |
| **Recursos**: que cada servicio tenga dónde correr y que los clientes no esperen a que se libere una CPU | El raylet reparte las CPU: un actor necesita una libre para colocarse. Los clientes de `run_in_workers()` declaran `num_cpus=0`. Las pruebas arrancan Ray con 2 CPU | `start_ray(num_cpus=0)`: `start_cluster()` falla con `GetTimeoutError` a los 120 s (sección "Recursos y placement"). [test_ray_cluster.py](tests/integration/test_ray_cluster.py) `test_concurrent_clients_lose_no_files`: 4 clientes corren a la vez con solo 2 CPU | ¿Hace falta algo más que un proceso por servicio y los límites del sistema operativo? ¿Cuántas peticiones concurrentes acepta cada servicio antes de rechazar? |
| **Monitoreo**: saber qué proceso atendió cada petición, si se repitió y seguir una operación de punta a punta | Solo el `pid()` de cada actor y los logs de Ray. No hay forma de seguir una petición | [Experimento del TagShard colgado](#experimento-un-tagshard-colgado): el reintento de `lookup` solo aparece en el log del raylet y el gateway no se entera de que su petición se ejecutó dos veces. [demo_clients.py](scripts/demo_clients.py) imprime el PID de cada actor y cliente | ¿Un `request_id` en cada operación, que cada servicio registre? ¿Logs estructurados y métricas por servicio: latencia, timeouts, reinicios y reintentos? |

Lo que no hace falta construir: un scheduler general, un almacén de resultados
como el de `ObjectRef` ni recomputación por linaje, porque el contenido no se
calcula: lo aporta el cliente.

Garantías que deben ser más fuertes que en el prototipo:

- Los metadatos deben sobrevivir a un reinicio, y a la pérdida de una máquina si
  se replican.
- Un `put` reintentado no debe crear otro archivo.
- Los cambios pendientes deben llegar al índice sin que nadie tenga que llamar a
  `repair_index()`.
- La recolección no debe poder cruzarse con un `put` de ningún cliente.
- Las réplicas deben estar en máquinas distintas, y una escritura solo se acepta
  con un mínimo de copias que hay que fijar.
- Toda llamada entre servicios debe tener timeout.

Preguntas abiertas:

- ¿Quién entrega los cambios pendientes al índice: el propio shard de metadatos,
  un proceso periódico o cada cliente al arrancar?
- ¿Cómo se coordina la recolección con los `put` de todos los clientes: un lock,
  marcas de escritura en curso o un tiempo de gracia antes de borrar?
- ¿Cuántas copias deben confirmar para aceptar un `put`?
- ¿Basta con que cada cliente tenga su propia vista de qué almacenes están vivos,
  o hace falta una membresía compartida?
- ¿Quién reinicia un shard de etiquetas caído, y qué hace un cliente con uno que
  responde lento pero sigue vivo?

## Preguntas de diseño del tema

Respuestas para el prototipo. Donde la versión nativa tendrá que decidir algo,
se dice.

### ¿Qué estado existe y qué nodos lo poseen o replican?

| Estado | Dueño | Copias |
| --- | --- | --- |
| Ficha de cada archivo (nombre, etiquetas, hash del contenido, versión y marca de borrado) y outbox de cambios pendientes para el índice | El `MetadataShard` que le toca por `sha256(file_id)` módulo el número de shards, 4 por defecto ([router.py](src/tagfs/router.py)) | Ninguna |
| Índice: los archivos de cada etiqueta, con la versión de cada entrada, también de las quitadas | El `TagShard` que le toca por `sha256(etiqueta)`, igual | Ninguna: se reconstruye desde las fichas |
| Bytes de cada contenido, identificados por su SHA-256 | Los nodos que indica el anillo de hashing consistente ([ring.py](src/tagfs/ring.py)) | 2 por defecto, en `StorageNode` distintos |
| Qué almacenes están vivos | El `ContentStore` de cada cliente | Una vista por cliente, sin coordinar |

Un archivo eliminado conserva su ficha con la marca de borrado, sin etiquetas.
Así su ID no se puede reutilizar.

### ¿Cuándo se confirma una operación?

- `put`: cuando al menos una réplica guardó los bytes y el shard de metadatos creó
  la ficha. Se intentan 2 réplicas y, si una falla, la siguiente del anillo.
- Un cambio sobre un archivo: cuando lo acepta su shard de metadatos.
- Un cambio por consulta: cada archivo, cuando lo acepta su shard. La operación
  responde cuando todos los shards con candidatos respondieron o fallaron.
- Antes de responder, el gateway entrega el cambio al índice. Si el índice
  respondió, cualquier cliente que consulte después ya ve el cambio. Si no
  respondió, la operación no falla, pero las consultas no lo ven hasta
  `repair_index()`.

### ¿Qué operaciones se pueden reintentar y cómo se tratan los duplicados?

La tabla de [Fallos y reintentos](#fallos-y-reintentos) lo detalla por operación.
En resumen:

- Las lecturas y `delete` se pueden repetir siempre.
- Los cambios de etiquetas y de nombre, y los cambios por consulta, no se aplican
  dos veces: un cambio sin efecto no sube la versión. Pero si otro cliente cambió
  el archivo entre los dos intentos, la repetición puede deshacer su cambio.
- `put` no se puede repetir: cada intento crea otro archivo. Es un límite conocido
  del contrato. La versión nativa necesita algo como un `request_id` que los
  metadatos recuerden.
- Los duplicados internos no tienen efecto: el índice descarta un cambio cuya
  versión ya conoce, y un almacén guarda cada contenido bajo su hash, así que los
  mismos bytes se guardan una sola vez.

### ¿Cómo se descubren los miembros y los servicios?

No hay descubrimiento: el número de shards y de almacenes se fija al arrancar.
`start_cluster()` crea los 12 actores y cada cliente recibe sus handles dentro del
`RayCluster`. Qué shard o almacén guarda cada clave lo calcula cada cliente, sin
preguntar a nadie. El único directorio es el GCS de Ray, en el que cada `TagShard`
busca por nombre a los shards de metadatos cuando arranca. Qué almacenes están
vivos lo aprende cada cliente cuando uno falla, y `reconcile()` readmite a los que
vuelven. En la versión nativa bastaría una configuración estática con la
dirección de cada servicio, porque el conjunto no cambia.

### ¿Qué pasa cuando falla un nodo?

En el prototipo un nodo es un proceso: matar un actor simula perderlo. La pérdida
de una máquina entera no se ha probado.

| Cae | Qué pasa | Evidencia |
| --- | --- | --- |
| Un `MetadataShard` | Sus archivos dan `ServiceUnavailable`; los de los demás shards siguen funcionando. `list()`, la recolección y las consultas con candidatos suyos fallan. En un cambio por consulta, sus candidatos salen en `unknown`. Sus fichas no tienen copia: si el proceso muere, se pierden | [test_end_to_end.py](tests/integration/test_end_to_end.py) `test_metadata_shard_failure_loses_its_files` y `test_a_change_by_query_reports_the_files_of_a_dead_metadata_shard_as_unknown`; [test_gateway_errors.py](tests/integration/test_gateway_errors.py) `test_hung_metadata_shard_puts_its_files_in_unknown_in_a_change_by_query` |
| Un `TagShard` | Ray lo reinicia y se rellena desde las fichas; no se pierde nada. Si se cuelga, la primera consulta tarda unos 9 s | [Experimento del TagShard colgado](#experimento-un-tagshard-colgado) y [test_gateway_index_failures.py](tests/integration/test_gateway_index_failures.py) |
| Un `StorageNode` | Con factor 2 todo sigue legible y los `put` nuevos van a los almacenes vivos | [test_end_to_end.py](tests/integration/test_end_to_end.py) `test_storage_nodes_fail_one_after_another` |
| Un cliente a mitad de un cambio | El cambio queda en el outbox y `repair_index()`, desde otro cliente, lo entrega | [test_gateway_index_failures.py](tests/integration/test_gateway_index_failures.py) `test_a_client_that_dies_before_telling_the_index_is_repaired` y `test_a_client_that_dies_in_the_middle_of_a_change_by_query_is_repaired` |

### ¿Qué pasa durante una partición y qué cambia al sanar?

Solo se simula en los almacenes: `disconnect()` hace que un almacén rechace las
peticiones. Mientras está aislado, cada cliente lo saca de su anillo, lee de la
otra réplica y escribe las copias nuevas en otros nodos. Al volver, `reconcile()`
lo readmite, deja cada contenido en las réplicas que indica el anillo y borra las
copias sobrantes y el contenido que nadie usa. Los números están en
[Experimentos y resultados](#experimentos-y-resultados).

Las fichas y el índice no tienen copias, así que una partición los vuelve
inalcanzables, pero no puede dejar dos copias distintas. Un `MetadataShard`
aislado se vería como uno colgado: `ServiceUnavailable` tras `metadata_timeout`.
Un `TagShard` aislado no respondería y el gateway pediría reiniciarlo; los cambios
no se perderían, porque siguen en el outbox. Esto es un supuesto, no se ha
provocado. Si la versión nativa replica las fichas, una partición sí podría
dejar copias distintas, y habrá que decidir quién acepta escrituras mientras dure.

Una ficha apunta a un hash y el contenido de un hash no cambia nunca. Por eso una
ficha no puede apuntar a otros bytes: como mucho, a unos que no se alcanzan
durante la partición, y entonces `get` da `ContentNotFound` hasta que el almacén
vuelve.

### ¿Qué garantías son de corrección y cuáles son objetivos de disponibilidad o rendimiento?

De corrección: deben cumplirse siempre, también con fallos y con clientes
concurrentes.

| Garantía | Evidencia |
| --- | --- |
| Dos cambios sobre un archivo no se pisan: cada cambio efectivo recibe su propia versión | Experimento de 50 clientes; [test_gateway_clients.py](tests/integration/test_gateway_clients.py) `test_concurrent_changes_by_query_lose_no_update` |
| `query`, `list(expr)` y los cambios por consulta deciden con las etiquetas actuales de la ficha: nunca devuelven ni tocan un archivo que no cumple la consulta, aunque el índice vaya atrasado | [test_gateway.py](tests/unit/test_gateway.py) `test_query_filters_stale_index_entries_with_metadata` y `test_a_change_by_query_checks_the_query_again_in_the_metadata`; [test_gateway_clients.py](tests/integration/test_gateway_clients.py) `test_delete_by_query_never_deletes_a_file_another_client_just_protected` |
| El índice no vuelve a un estado viejo: un cambio que llega tarde o repetido no tiene efecto | [test_convergence.py](tests/unit/test_convergence.py), 200 secuencias aleatorias entregadas desordenadas y repetidas |
| `get` nunca devuelve bytes que no coinciden con su hash | [test_gateway.py](tests/unit/test_gateway.py) `test_get_detects_corrupted_content` |
| Un archivo eliminado no vuelve y su ID no se reutiliza | [test_metadata_shard.py](tests/unit/test_metadata_shard.py) `test_deleted_id_is_never_reused` y `test_deleted_file_behaves_as_not_found` |
| Una petición mal formada no cambia nada | [gateway_contract.py](tests/gateway_contract.py) |

Hay una excepción conocida: una recolección que se cruza con un `put` puede borrar
los bytes de un archivo recién creado. Por eso hay que detener las escrituras
antes de recolectar, y hoy nada lo impide.

De disponibilidad y rendimiento: son objetivos y pueden fallar sin que el
sistema quede incorrecto.

- Una consulta ve un cambio en cuanto el cambio termina, si el índice respondió.
- Con factor 2, un almacén caído no impide leer ni escribir.
- Un `TagShard` caído o colgado se recupera solo; si está colgado, en unos 9 s.
- La caída de un shard de metadatos solo afecta a sus archivos.
- Ninguna espera es indefinida: `metadata_timeout` es de 10 s e `index_timeout`,
  de 5 s, más el reinicio, hasta `restart_timeout` (120 s).
- Un cambio por consulta hace una sola llamada por shard, para que cambiar miles
  de archivos no supere `index_timeout`.
- Un `put` se acepta con una sola copia: se prefiere aceptar la escritura a
  esperar a que haya dos.

### ¿Qué evidencia usará la Fase 3?

- El contrato y las garantías son pruebas de caja negra que no dependen de Ray.
  La versión nativa debe cumplir las mismas aserciones de
  [gateway_contract.py](tests/gateway_contract.py) y
  [maintenance_checks.py](tests/maintenance_checks.py): con un cliente de Python
  que hable con ella, solo cambia el fixture que da el gateway; si no, se
  traducen. Así se comparan las dos versiones.
- El modelo de [test_end_to_end.py](tests/integration/test_end_to_end.py) calcula
  lo que el sistema debería contener sin el sistema. Sirve para comparar
  cualquier versión tras operaciones y fallos aleatorios.
- Las carreras de [test_gateway_clients.py](tests/integration/test_gateway_clients.py)
  comprueban invariantes que no dependen de cómo se reparte el trabajo: ningún
  cambio se pierde y nada se borra si ya no cumplía la consulta.
- Los experimentos con predicción previa y datos guardados, como el del
  [TagShard colgado](#experimento-un-tagshard-colgado), se repetirán con la
  versión nativa para comparar tiempos de recuperación y archivos perdidos.
- Para producir esa evidencia, la versión nativa debe exponer un `request_id` por
  operación, registros de timeouts, reinicios y reintentos, cuántos cambios hay
  pendientes en cada outbox y cuánto tarda un cambio en verse en las consultas.
- Falta evidencia de: la recolección cruzada con un `put`, que se observó pero no
  tiene script en el repositorio; la caída de una máquina; y una partición de red
  real.

## Exit gate de la Fase 1

### 1. ¿Qué problema resuelve el sistema?

Guarda archivos y permite encontrarlos por etiquetas en lugar de por carpetas. Un
archivo puede estar en muchas colecciones a la vez, y una colección es una
consulta, como `foto AND 2024 AND NOT borrador`. Cada archivo tiene un ID estable,
un contenido, un nombre y un conjunto de etiquetas. Se puede crear, eliminar,
listar y cambiar etiquetas, también sobre todos los archivos que cumplen una
consulta.

Lo difícil es que esté distribuido. Los bytes, las fichas y el índice viven en
procesos distintos, con dueños y copias distintos, y uno puede ir por delante de
otro. El sistema tiene que mantenerlos coherentes con clientes concurrentes, con
procesos que mueren y con particiones.

### 2. ¿Qué comportamiento externo debe sobrevivir al rediseño?

El [contrato externo](#contrato-externo) completo:

- Las once operaciones del cliente, con sus resultados (`StoredFile`, `FileInfo`
  y `BulkResult`) y sus errores.
- Las reglas: un ID nuevo por `put`, versiones que solo suben con un cambio
  efectivo, etiquetas normalizadas, la gramática de las consultas, la validación
  antes de guardar y un `delete` que se puede repetir.
- Cómo se comportan los cambios por consulta: se decide archivo por archivo al
  cambiarlo, el resultado trae `done` y `unknown`, y no se deshace nada.
- Qué significa `ServiceUnavailable` durante un cambio: puede haberse hecho o no.
- Las garantías del mantenimiento, pero no sus métodos.

Puede cambiar todo lo demás: cómo se arranca el sistema y se obtiene un gateway,
cuántos shards hay, los valores de los timeouts, el factor de replicación y los
cuatro métodos de mantenimiento. Y debe mejorar lo que el contrato todavía no
promete: que reintentar un `put` no cree otro archivo y que un cambio reintentado
llegue al índice.

### 3. ¿Qué comportamiento viene de nuestro código?

Todo lo que el sistema decide. Ray ejecuta los procesos, los reinicia y lleva los
datos de uno a otro, pero no decide dónde va cada dato, qué es correcto ni qué se
repite.

| Comportamiento | Dónde |
| --- | --- |
| Qué shard guarda cada archivo y cada etiqueta | [router.py](src/tagfs/router.py) |
| Las fichas como fuente de verdad: versiones, cambios sin efecto que no suben la versión, IDs que no se reutilizan y el borrado como marca | [metadata_shard.py](src/tagfs/metadata_shard.py) |
| Qué etiqueta y qué consulta son válidas, y la gramática de las consultas | [model.py](src/tagfs/model.py), [query.py](src/tagfs/query.py) |
| Consultar en dos pasos: candidatos del índice y decisión con las fichas | [query.py](src/tagfs/query.py), [gateway.py](src/tagfs/gateway.py) |
| Los cambios por consulta, comprobados dentro del shard, con `done` y `unknown` | [metadata_shard.py](src/tagfs/metadata_shard.py), [gateway.py](src/tagfs/gateway.py) |
| Que el índice converja: entradas con versión, también las quitadas, y entregas por lotes | [tag_shard.py](src/tagfs/tag_shard.py) |
| El outbox con confirmación, los pendientes con el estado actual, `repair_index()` y `verify_index()` | [metadata_shard.py](src/tagfs/metadata_shard.py), [gateway.py](src/tagfs/gateway.py) |
| Rellenar un `TagShard` desde las fichas cuando arranca, saltándose los shards muertos. Ray solo vuelve a arrancar el proceso | [ray_runtime.py](src/tagfs/ray_runtime.py) |
| Decidir que un `TagShard` está muerto: no respondió en `index_timeout` | [gateway.py](src/tagfs/gateway.py) |
| El orden de una escritura: contenido, ficha e índice | [gateway.py](src/tagfs/gateway.py) |
| Identificar el contenido por su SHA-256, de modo que los mismos bytes se guardan una vez | [content.py](src/tagfs/content.py), [content_store.py](src/tagfs/content_store.py) |
| Elegir las réplicas con hashing consistente y pasar al siguiente nodo si uno falla | [ring.py](src/tagfs/ring.py), [content_store.py](src/tagfs/content_store.py) |
| Verificar cada lectura con el hash y quedarse con la primera réplica válida | [content_store.py](src/tagfs/content_store.py), [gateway.py](src/tagfs/gateway.py) |
| La vista de almacenes vivos de cada cliente, la recolección y la reconciliación tras una partición | [content_store.py](src/tagfs/content_store.py) |
| Traducir los errores de Ray a los del contrato | [gateway.py](src/tagfs/gateway.py) |

Hay decisiones nuestras que usan mecanismos de Ray: qué se reintenta
(`max_retries=0` en los clientes, reinicios y reintentos en el `TagShard`) y qué
peticiones salen a la vez. La decisión es del código; el mecanismo, de Ray, y
está en la pregunta 4.

### 4. ¿Qué viene de Ray?

El [mapa de dependencias](#retirada-de-ray) lo detalla por requisito. En resumen:

- **Lo que Ray da y la versión nativa tendrá que construir:** ejecución remota
  con plazos y errores, un proceso con su estado y su cola de llamadas para cada
  shard y almacén, el reinicio del `TagShard` con la misma identidad, la
  búsqueda de los shards por nombre y el envío de los bytes a varias réplicas
  con una sola serialización.
- **Lo que Ray da y no hará falta:** un scheduler general, un almacén de
  resultados y la recomputación por linaje. Qué nodo guarda cada clave ya lo
  decide la aplicación, y ningún resultado sobrevive a su operación.
- **Lo que Ray no da y hoy falta:** persistencia de los metadatos, réplicas en
  máquinas distintas, deduplicación de un `put` reintentado y un monitoreo que
  siga una petición de punta a punta. Hoy solo hay PIDs y logs de Ray.
- **Lo que ya no se filtra al cliente:** ningún error de Ray llega al contrato;
  el gateway los traduce a `ServiceUnavailable` o al error original.

### 5. ¿Qué observamos y qué suponemos?

Observado es lo que reproduce una prueba o un experimento del repositorio.
Supuesto es lo que deducimos del código y de la documentación de Ray, sin
haberlo provocado.

| Escenario | Tipo | Qué pasa | Evidencia |
| --- | --- | --- | --- |
| Un `TagShard` se reinicia | Observado | Ray lo arranca en otro proceso y lo rellena desde los metadatos. Colgado, la primera consulta tarda unos 9 s y devuelve todo: 0 archivos perdidos y el cliente no ve ningún error | [Experimento del TagShard colgado](#experimento-un-tagshard-colgado) (6 JSON); [test_gateway_index_failures.py](tests/integration/test_gateway_index_failures.py) |
| Un shard de metadatos colgado | Observado | No se reinicia. Cada operación que lo necesita falla con `ServiceUnavailable` tras `metadata_timeout`, y `list()`, las consultas y la recolección lo necesitan siempre | [test_gateway_errors.py](tests/integration/test_gateway_errors.py) `test_hung_metadata_shard_raises_service_unavailable_in_time`: con 1 s de plazo, cada operación falla en menos de 6 s |
| Un cambio por consulta mientras otros clientes cambian los mismos archivos | Observado | Un cliente borra lo que no está protegido mientras otros dos protegen 120 archivos, en 3 rondas: cada archivo acaba protegido y vivo, o borrado y con `FileNotFound` al protegerlo, nunca las dos cosas. Con 4 clientes agregando por consulta a la vez, ningún cambio se pierde y cada uno recibe su propia versión | [test_gateway_clients.py](tests/integration/test_gateway_clients.py) |
| Muere o se cuelga un shard de metadatos durante un cambio por consulta | Observado | Los demás shards hacen su parte y los candidatos del shard que falló salen en `unknown`, sin error. Si está colgado, la operación responde tras `metadata_timeout` | [test_end_to_end.py](tests/integration/test_end_to_end.py) `test_a_change_by_query_reports_the_files_of_a_dead_metadata_shard_as_unknown`; [test_gateway_errors.py](tests/integration/test_gateway_errors.py) `test_hung_metadata_shard_puts_its_files_in_unknown_in_a_change_by_query`: con 1 s de plazo, responde en menos de 6 s |
| Recolectar durante un `put` | Observado | La recolección borra los bytes de un `put` que aún no tiene ficha. Ese archivo queda con ficha pero sin contenido: 22 de 800 archivos perdidos | Observación del equipo; falta el script o el JSON que la reproduzca en el repositorio. El riesgo está documentado en `collect_garbage()` |
| Lectura con un almacén caído | Observado | Con factor 2, todo sigue legible y los `put` nuevos guardan 2 copias en los almacenes vivos. Al caer las dos réplicas, el contenido se pierde pero la ficha sigue: el archivo aparece en `list()` y en las consultas, y `get` da `ContentNotFound` | [test_end_to_end.py](tests/integration/test_end_to_end.py) `test_storage_nodes_fail_one_after_another`; demo: 20 de 20 legibles con factor 2 y 14 de 20 con factor 1 |
| Cae una máquina | Supuesto | Se pierde todo lo que corre en ella. En local son todos los actores; con varias máquinas, nada impide que las dos réplicas de un contenido estén en la misma | Ninguna: todo corre en una máquina, y matar un actor solo simula perder un proceso |
| Cae el GCS | Supuesto | Sin tolerancia a fallos del GCS, Ray no puede reiniciar actores, crear otros ni resolver nombres: un `TagShard` que muera no volvería | Ninguna: no se ha provocado. Se deduce de la documentación de Ray |
| Partición de red real | Supuesto | Un almacén aislado se comporta como uno que rechaza peticiones: se saca del anillo y `reconcile()` lo readmite al volver | Solo simulada: `disconnect()` hace que el almacén rechace peticiones, pero no toca la red. [test_partitions.py](tests/integration/test_partitions.py) |
| Metadatos o índice aislados | Supuesto | Unos metadatos aislados se verían como colgados: `ServiceUnavailable` tras `metadata_timeout`. Un índice aislado no respondería a tiempo y el gateway pediría reiniciarlo. Si el `ray.kill` no le llega, porque también está aislado del GCS, las consultas fallarían tras `restart_timeout` (120 s); los cambios no se pierden, quedan en el outbox | Ninguna: `disconnect()` solo existe en los almacenes |
