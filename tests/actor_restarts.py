"""Simula la caída de un actor que Ray reinicia y espera al proceso nuevo."""

import time

import ray


def crash_and_wait_for_restart(actor, seconds=60):
    """Mata el actor como un fallo real y vuelve cuando responde otro proceso.

    ray.kill no espera a que el actor muera: justo después de llamarlo suele
    responder todavía el proceso viejo. Sin esta espera, un test que lee el
    índice tras la caída lee el índice de antes y no la reconstrucción.
    """
    before = ray.get(actor.pid.remote())
    ray.kill(actor, no_restart=False)
    deadline = time.monotonic() + seconds
    while ray.get(actor.pid.remote(), timeout=seconds) == before:
        if time.monotonic() > deadline:
            raise TimeoutError("El actor no se reinició")
        time.sleep(0.1)
