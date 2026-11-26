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
python -m pytest -q             # suite completa, unos 8 minutos
python -m pytest tests/unit -q  # solo la lógica, sin Ray
python scripts/demo.py          # operaciones, fallos, partición y concurrencia
python scripts/demo_clients.py  # varios clientes a la vez y el proceso de cada actor
```

Ray arranca un clúster local: son varios procesos en la misma máquina, no varias
máquinas.

## Contrato externo

Es lo que la versión nativa debe conservar. Está escrito como pruebas en
[tests/gateway_contract.py](tests/gateway_contract.py), que se ejecutan dos veces:
sin Ray, con shards locales ([tests/unit/test_gateway.py](tests/unit/test_gateway.py)),
y con actores reales ([tests/integration/test_contract.py](tests/integration/test_contract.py)).

```python
from tagfs.ray_runtime import start_cluster, stop_cluster

cluster = start_cluster()       # arranca Ray si hace falta y crea los actores
gateway = cluster.gateway()     # cada cliente crea el suyo
file_id = gateway.put("nota.txt", b"hola", {"Documento", "trabajo"})
gateway.query("documento AND NOT borrador")  # [file_id]
stop_cluster()
```

| Operación | Resultado |
| --- | --- |
| `put(name, content, tags=())` | Crea un archivo y devuelve su ID. `content` debe ser `bytes` |
| `get(file_id)` | Devuelve `StoredFile(name, content, tags, version)`, que también se puede usar como tupla |
| `add_tags(file_id, tags)` | Agrega etiquetas y devuelve la versión resultante |
| `remove_tags(file_id, tags)` | Quita etiquetas y devuelve la versión resultante |
| `rename(file_id, new_name)` | Cambia el nombre y devuelve la versión resultante |
| `delete(file_id)` | Elimina el archivo de forma lógica |
| `query(expr)` | Devuelve, ordenados, los IDs que cumplen la expresión |
| `list()` | Devuelve, ordenados, los IDs de todos los archivos vivos |
| `collect_garbage()` | Borra el contenido que ya no usa ningún archivo y dice cuántos borró |
| `reconcile()` | Repara la ubicación de las copias tras fallos o particiones y devuelve un informe |
| `repair_index()` | Entrega al índice los cambios pendientes y dice cuántos entregó |
| `verify_index()` | Compara el índice con los metadatos sin cambiar nada: devuelve lo que falta (`missing`) y lo que sobra (`extra`) |

Reglas:

- Cada `put` genera un ID nuevo, aunque el nombre y los bytes se repitan. Dos
  archivos pueden compartir los mismos bytes y tener metadatos independientes.
- La versión empieza en 1 y sube con cada cambio efectivo. Agregar una etiqueta
  que ya está, quitar una que no está o repetir el nombre no la cambia.
- Las etiquetas se guardan en minúsculas. Se rechazan las vacías, las que tienen
  espacios o paréntesis y las palabras `and`, `or` y `not`.
- Las consultas admiten `AND`, `OR`, `NOT` y paréntesis, y cada rama necesita una
  parte positiva: `foto AND NOT borrador` vale, `NOT borrador` no.
- Repetir `delete` sobre un archivo eliminado no cambia nada. Eliminar no borra
  los bytes al momento, porque otro archivo puede usarlos; lo hace la recolección.
- Un cambio queda hecho cuando lo aceptan los metadatos. Si después el índice no
  responde, el cliente no recibe error; el cambio queda pendiente y las consultas
  no lo ven hasta que alguien llame a `repair_index()`.

Errores que ve el cliente:

| Error | Cuándo |
| --- | --- |
| `FileNotFound` | El archivo no existe, fue eliminado o el ID nunca existió |
| `InvalidTag`, `InvalidQuery` | La etiqueta o la consulta están mal formadas; no se guarda nada |
| `ContentNotFound` | La ficha existe, pero ninguna réplica alcanzable tiene los bytes |
| `ContentCorrupted` | Los bytes no coinciden con su hash |
| `StorageUnavailable` | Ningún almacén pudo guardar el contenido de un `put` |
| `ServiceUnavailable` | Un shard de metadatos que la operación necesita murió o no respondió en `metadata_timeout` (10 s), o un shard de etiquetas no respondió ni después de reiniciarlo. Si ocurre durante un cambio, el cambio puede haberse hecho o no |

Todos son subclases de `TagFSError` y ninguno es un tipo de Ray: el gateway
traduce los errores de Ray antes de que lleguen al cliente, así que el contrato
no cambia al quitar Ray.

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
| `repair_index` | Seguro: entregar dos veces un cambio no tiene efecto |
| `collect_garbage`, `reconcile` | Seguro, si no se cruzan con un `put` |
| `TagShard.apply`, `TagShard.lookup` | Seguro: la versión descarta duplicados y `lookup` solo lee |
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

Cada fila completa la frase "Ray me permite expresar ___; debajo necesita ___; mi
aplicación exige ___; en la versión nativa lo resolveré mediante ___ porque ___".

| Ray permite expresar | Debajo necesita | La aplicación exige | En la versión nativa, y por qué |
| --- | --- | --- | --- |
| Actores con estado | Crear procesos, darles identidad y enrutar las llamadas | 12 dueños fijos de partes del estado | Un servicio Go por shard o almacén, porque el reparto por hash necesita conocer la lista desde el principio |
| Un método a la vez por actor | Una cola de llamadas por actor | Las invariantes de la tabla de concurrencia | Exclusión mutua en cada shard, porque los handlers de Go atienden peticiones en paralelo |
| `.remote()`, `ray.get()`, `ray.wait()` | Serializar, transportar, esperar y propagar excepciones | Petición y respuesta con destinatario conocido, timeout en todas las llamadas y errores del contrato | gRPC con plazos y códigos de error propios, porque no hace falta nada más general |
| `ray.put()` y `ObjectRef` | Un object store que evita serializar dos veces y lleva los bytes a cada réplica | Enviar los bytes a N réplicas; ningún resultado se usa después de su operación | Los bytes van en la petición a cada réplica y no hay almacén de resultados, porque ningún `ObjectRef` sobrevive a su operación |
| `RayCluster` con handles y actores con nombre | Un registro de Ray que sabe dónde vive cada actor | Que clientes y shards de etiquetas encuentren 12 servicios fijos | Configuración estática con las direcciones, porque el conjunto no cambia; un registro solo si se añaden nodos en caliente |
| Scheduler | Elegir proceso y máquina para cada actor y task | Qué nodo guarda cada clave ya lo decide la aplicación; lo que falta es separar las réplicas | Asignación estática por configuración, con la máquina de cada nodo, porque no hay trabajo que repartir de forma dinámica |
| `RayActorError` | Saber cuándo muere un proceso y avisar a quien lo llama | Decidir cuándo sacar un almacén del anillo o reiniciar un shard de etiquetas | Timeouts y conexiones rechazadas, tratando igual "lento" y "muerto", porque sin Ray no se distinguen |
| `max_restarts` y `ray.kill(no_restart=False)` | Relanzar el proceso con la misma identidad y repetir `__init__` | Que un shard de etiquetas caído vuelva y se rellene desde los metadatos | Un supervisor que relance el servicio en la misma dirección; al arrancar lee los metadatos, como hoy |
| `max_task_retries` en el `TagShard` | Repetir las llamadas en curso tras un reinicio | Que no se pierdan `lookup` ni `apply` | El cliente reintenta con timeout, porque las dos operaciones son idempotentes |
| `max_retries=0` en los clientes | Nada: desactiva la repetición de Ray | Que repetir un `put` no cree otro archivo | Un `request_id` del cliente y deduplicación en el shard de metadatos, porque sin eso cualquier reintento crea un duplicado |
| Actores ligados al proceso que los creó | Registrar quién es dueño de cada actor | Que el estado no dependa de un proceso concreto | Servicios independientes, cada uno con su ciclo de vida |
| Estado en memoria del actor | Nada: Ray no persiste ni replica esa memoria | Que los metadatos sobrevivan a un reinicio | Un log por shard con replay al arrancar, porque los metadatos no se pueden reconstruir desde otra parte |
| `runtime_env` | Copiar el paquete Python a cada worker | Tener el código en cada máquina | Un binario desplegado en cada máquina |

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
