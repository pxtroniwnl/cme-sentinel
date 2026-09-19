# AGENTS.md — CME Sentinel

Contexto para agentes que trabajen en este repo. Léelo completo antes de
tocar código. El usuario trabaja en **español**: responde en español salvo que
indique lo contrario.

## Propósito

Proyecto de investigación causal **CME → efecto satelital**. Objetivo final:
demostrar con datos históricos que las eyecciones de masa coronal afectan a
los satélites (decaimiento orbital por drag y fallas electrónicas) y, a
futuro, generar **aviso anticipado**.

Cadena física (el modelo mental de todo el proyecto):

- **Vía orbital**: CME → tormenta geomagnética (Dst↓ / Kp↑) → calentamiento de
  la termósfera → drag↑ → decaimiento/reeentry (visible en `MEAN_MOTION↑`,
  `PERIAPSIS↓`, `DECAY_DATE`).
- **Vía electrónica**: CME/SEP → partículas energéticas → upsets / daño por
  radiación (no visible en TLEs; requiere telemetría/anomalías).
- Caso de validación canónico: **Starlink feb-2022** (~40 sats reentrados tras
  una CME).

## Fuentes de datos (mapa de verdad)

| Fuente | Rol en la cadena | Estado |
|---|---|---|
| Space-Track `gp_history` | Efecto (órbita/decaimiento) | ✅ Descargado (`data/gp_history`) |
| OMNI (NASA GSFC) | Driver continuo + Dst/Kp/protones | ⏳ Planificado (`scripts/fetch_omni.py`) |
| DONKI (NASA CCMC) | Eventos discretos (CME/GST/SEP/flares/HSS) | ⏳ Planificado (`scripts/fetch_donki.py`; requiere `NASA_API_KEY`) |
| ESA Anomaly Dataset | Benchmark de telemetría — **fuera de la correlación causal** | 📄 Solo documentado |

**Regla de oro**: el ESA Anomaly Dataset tiene el **eje temporal anonymizado**
(paper: la anonimización "impide esperar anomalías en tiempos específicos,
p. ej. durante actividad solar aumentada"). **Nunca** lo alinees contra
DONKI/OMNI ni lo uses en el análisis causal.

## Layout

- `README.md` — doc de referencia completa (fuente de verdad del data model y
  del estado de cada fase).
- `scripts/fetch_gp_history.py` — descargador del catálogo orbital; es el
  **patrón de estilo** que deben seguir los futuros descargadores.
- `notebooks/01_validate_and_explore.ipynb` — validación y exploración de
  `gp_history`.
- `data/gp_history/year=YYYY/part-*.parquet` — catálogo orbital (git-ignored).
- `data/.progress.json` — ventanas completadas para resume (git-ignored).
- `.env` — credenciales (`SPACE_TRACK_EMAIL`, `SPACE_TRACK_PASSWORD`, y a futuro
  `NASA_API_KEY`). **Nunca commitear.**
- `.env.example` — plantilla de credenciales; mantenela al día.

`data/`, `.env`, `.venv/` y cualquier `*.parquet` están en `.gitignore`.

## Comandos

- Setup: `python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt`
- Descarga orbital: `python scripts/fetch_gp_history.py` — resume automático;
  `--limit 5` para pruebas; `--reset` solo intencional (clase "1 / lifetime").
- Credenciales: copiar `.env.example` → `.env` y completar.

## Convenciones (aplicar a todo script nuevo)

- Espejo de `fetch_gp_history.py`: `argparse` con `--help` descriptivo,
  `logging` (`%(asctime)s [%(levelname)s] %(message)s`), **retries con backoff
  exponencial** y **resume** vía claves estables en `data/.progress.json`.
- Salida en **Parquet particionado por año** (`year=YYYY/`), columnas
  normalizadas (datetimes en UTC, `Int64` nullable para enteros).
- Cortesía con APIs ajenas: pausa mínima entre requests, respetar rate limits,
  ventanas que se subdividen si fallan.
- **No agregar comentarios** al código salvo que se pidan.
- Cada fuente vive en su subcarpeta bajo `data/` (`data/gp_history`, después
  `data/omni`, `data/donki`).

## Estados del proyecto

1. ✅ Catálogo orbital histórico completo descargado.
2. ⏳ Recolectar OMNI (horario 1963+) y DONKI (núcleo causal).
3. ⏳ Cruzar tormentas (OMNI/DONKI) contra decaimientos (Space-Track) y validar
   con el caso Starlink feb-2022.

## Política de datos

- Space-Track `GP_HISTORY` es clase "1 / lifetime": descargar una vez y
  guardar local; no re-ejecutar descargas completas.
- USSPACECOM permite redistribución de datos SSA básicos citando
  `USSPACECOM/18 SDS`.
- Todos los timestamps son **UTC**.