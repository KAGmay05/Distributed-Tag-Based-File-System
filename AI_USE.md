```yaml
ai_use:
  used: true
  tools:
    - Claude Code
  activities:
    - discusión de la arquitectura y del contrato del gateway
    - asistencia en la implementación
    - generación de tests
    - revisión del código y de los tests
    - documentación (README)
  verification:
    - 549 tests con pytest (436 sin Ray y 113 con Ray 2.60.0 real); la suite completa pasa
    - los mismos tests de contrato se ejecutan contra el gateway sin Ray y con Ray
    - mutación del código: se rompió a propósito la reconstrucción del índice y los tests de reinicio del TagShard seguían pasando; se reescribieron hasta que la detectaron (tests/actor_restarts.py)
    - experimento del TagShard colgado con la predicción commiteada antes de ejecutar, tres repeticiones por escenario y los datos en experiments/results/
    - ejecuciones de las demos con Ray real
    - revisión manual de cada cambio antes de hacer commit
    - revisión manual contra los documentos del curso
```
