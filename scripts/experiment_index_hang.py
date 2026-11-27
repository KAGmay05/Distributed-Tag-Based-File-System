"""Experimento: un TagShard colgado. ¿Cuánto tarda el sistema en recuperarse?

Arranca el clúster, guarda archivos con una etiqueta, cuelga el TagShard de esa
etiqueta y consulta hasta obtener la respuesta correcta. Con --control hace lo
mismo sin colgar nada. Cada ejecución guarda un JSON en experiments/results/.

Uso, desde la raíz del proyecto:
    python scripts/experiment_index_hang.py --fault
    python scripts/experiment_index_hang.py --control
"""

from __future__ import annotations

import argparse
import json
import platform
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.stdout.reconfigure(encoding="utf-8")  # la consola de Windows no siempre usa UTF-8

import ray  # noqa: E402

from tagfs import ray_runtime  # noqa: E402
from tagfs.model import TagFSError  # noqa: E402
from tagfs.ray_runtime import start_cluster, stop_cluster  # noqa: E402

FILES = 20
TAG = "exp-hang"
RESULTS = ROOT / "experiments" / "results"
# Si no se recupera en este tiempo, se da la ejecución por fallida.
GIVE_UP_SECONDS = 300


def hang(self):
    # Se ejecuta dentro del actor y lo deja ocupado: las peticiones siguientes esperan detrás.
    time.sleep(3600)


def tag_on_another_shard(gateway) -> str:
    """Una etiqueta que cae en otro TagShard, para ver si el cuelgue le afecta."""
    hung = gateway._tag_index(TAG)
    for i in range(1000):
        candidate = f"exp-otro-{i}"
        if gateway._tag_index(candidate) != hung:
            return candidate
    raise RuntimeError("No hay etiqueta en otro shard")


def timed(function):
    started = time.monotonic()
    result = function()
    return result, time.monotonic() - started


def run(fault: bool) -> dict:
    cluster = start_cluster()
    gateway = cluster.gateway()
    other_tag = tag_on_another_shard(gateway)
    shard = gateway._tag_shard_for(TAG)

    expected = sorted(gateway.put(f"{i}.dat", f"archivo {i}".encode(), {TAG}) for i in range(FILES))
    other_expected = sorted(
        gateway.put(f"otro-{i}.dat", f"otro {i}".encode(), {other_tag}) for i in range(FILES)
    )
    assert gateway.query(TAG) == expected and gateway.query(other_tag) == other_expected
    pid_before = ray.get(shard.pid.remote())

    if fault:
        # max_task_retries=0: si Ray repitiera esta llamada en el actor nuevo, también
        # lo colgaría y no se recuperaría nunca.
        shard.__ray_call__.options(max_task_retries=0).remote(hang)

    # Una consulta a otro shard mientras el primero está colgado.
    other_found, other_seconds = timed(lambda: gateway.query(other_tag))

    # Consultar hasta obtener la respuesta correcta.
    attempts = []
    started = time.monotonic()
    found: list[str] = []
    while time.monotonic() - started < GIVE_UP_SECONDS:
        try:
            found, seconds = timed(lambda: gateway.query(TAG))
            attempts.append({"seconds": round(seconds, 3), "found": len(found)})
            if found == expected:
                break
        except TagFSError as error:
            attempts.append({"error": type(error).__name__, "message": str(error)})
    recovered = found == expected
    seconds_to_correct = time.monotonic() - started if recovered else None

    pid_after = ray.get(shard.pid.remote())
    check = gateway.verify_index()
    result = {
        "experiment": "index_hang",
        "mode": "fault" if fault else "control",
        "finished_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "ray_version": ray.__version__,
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "config": {
            "metadata_shards": len(gateway._metadata),
            "tag_shards": len(gateway._tags),
            "storage_nodes": len(cluster.storage),
            "replication_factor": cluster.replication_factor,
            "metadata_timeout": gateway._metadata_timeout,
            "index_timeout": gateway._index_timeout,
            "restart_timeout": gateway._restart_timeout,
            "worker_start_timeout": ray_runtime.WORKER_START_TIMEOUT_SECONDS,
            "files": FILES,
        },
        "tag": TAG,
        "tag_shard": gateway._tag_index(TAG),
        "recovered": recovered,
        "seconds_to_first_correct_query": round(seconds_to_correct, 3) if recovered else None,
        "attempts": attempts,
        "pid_before": pid_before,
        "pid_after": pid_after,
        "pid_changed": pid_before != pid_after,
        "expected_files": len(expected),
        "found_files": len(set(found) & set(expected)),
        "lost_files": len(set(expected) - set(found)),
        "verify_index": {"missing": len(check.missing), "extra": len(check.extra)},
        "other_shard_query": {
            "tag": other_tag,
            "tag_shard": gateway._tag_index(other_tag),
            "seconds": round(other_seconds, 3),
            "correct": other_found == other_expected,
        },
    }
    stop_cluster()
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--fault", action="store_true", help="colgar el TagShard")
    mode.add_argument("--control", action="store_true", help="no colgar nada")
    args = parser.parse_args()

    result = run(fault=args.fault)
    RESULTS.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    path = RESULTS / f"index_hang_{result['mode']}_{stamp}.json"
    path.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2, ensure_ascii=False))
    print(f"\nGuardado en {path.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
