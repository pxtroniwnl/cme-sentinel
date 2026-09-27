# AGENTS.md — CME Sentinel

Contexto para agentes que trabajen en este repo. Léelo completo antes de
tocar código. El usuario trabaja en **español**: responde en español salvo que
indique lo contrario.

## Propósito

Proyecto de investigación causal **CME → efecto satelital**. Objetivo final:
demostrar con datos históricos que las eyecciones de masa coronal afectan a
los satélites (decaimiento orbital por drag y fallas electrónicas) y, a
futuro, generar **aviso anticipado** (predicción de tormentas + riesgo de
daño por satélite específico) y **visualizaciones 3D**. Ver README §1 y §6.

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
| Space-Track `gp_history` | Efecto (órbita/decaimiento) | ⚠️ **NO en disco** (se borró para liberar espacio). Re-descargar **acotado** con `--start` |
| OMNI (NASA GSFC) | Driver continuo + SYM-H/AE/protones | ✅ Descargado 2012-01-01 → 2026-09-03, 128.617 filas horarias, 20 MB (`scripts/fetch_omni.py`, HAPI `cdaweb.gsfc.nasa.gov/hapi`, dataset `OMNI_HRO_5MIN`). Diccionario: `docs/datos_omni.md` |
| DONKI (NASA CCMC) | Eventos discretos (CME/GST/SEP/flares/HSS) | ✅ Script listo, **sin descargar** (`scripts/fetch_donki.py`; requiere `NASA_API_KEY`) |
| Gunter's Space Page | Estado/falla por satélite — capa narrativa extra | ✅ Descargado (`data/gunter`), ver `fetch_gunter.py` |
| ESA Anomaly Dataset | Benchmark de telemetría — **fuera de la correlación causal** | 📄 Solo documentado |

**Regla de oro**: el ESA Anomaly Dataset tiene el **eje temporal anonymizado**
(paper: la anonimización "impide esperar anomalías en tiempos específicos,
p. ej. durante actividad solar aumentada"). **Nunca** lo alinees contra
DONKI/OMNI ni lo uses en el análisis causal.

## Layout

- `README.md` — doc de referencia completa en inglés (fuente de verdad del
  data model, del pipeline §2, de la compaginación §3 y del estado de cada
  fase).
- `README.es.md` — traducción completa del README al español; misma
  estructura; mantenela sincronizada si se edita el README.
- `docs/proceso_extraccion_gunter.md` — crónica paso a paso de la extracción
  de Gunter's (crawl, dataset de fallas, export tabular).
- `docs/datos_gunter.md` — diccionario de datos y estadísticas de los
  artefactos en `data/gunter` (`tables`, `incidents`, `gunter_tabular`,
  `meta/pages`, `pages/`).
- `docs/datos_omni.md` — diccionario de columnas de `data/omni`, regla de
  agregación 5 min → horario, tabla de fill values y validación ejecutada.
- `scripts/fetch_gp_history.py` — descargador del catálogo orbital; es el
  **patrón de estilo** que deben seguir los futuros descargadores.
- `scripts/fetch_omni.py` — OMNI horario 1963+ vía HAPI (público, sin key).
- `scripts/fetch_donki.py` — eventos discretos DONKI, JSON flateado.
- `scripts/fetch_gunter.py` — crawler completo de Gunter's (raw + tablas).
- `notebooks/01_validate_and_explore.ipynb` — validación y exploración de
  `gp_history`.
- `notebooks/02_gunter_tabular.ipynb` — export tabular de Gunter's (1 fila
  por objeto, **sin clasificación** de causa; ya ejecutado).
- `data/gp_history/year=YYYY/part-*.parquet` — catálogo orbital (git-ignored).
- `data/gunter/` — páginas crudas (`pages/`, `meta/pages.parquet`),
  `tables.parquet`, `incidents.parquet` y el export tabular
  `gunter_tabular.parquet` (+`.csv`) de `notebooks/02_gunter_tabular.ipynb`
  (todo git-ignored).
- `data/.progress.json` — ventanas completadas para resume (git-ignored).
- `.env` — credenciales (`SPACE_TRACK_EMAIL`, `SPACE_TRACK_PASSWORD`,
  `NASA_API_KEY`). **Nunca commitear.**
- `.env.example` — plantilla de credenciales; mantenela al día.

`data/`, `.env`, `.venv/` y cualquier `*.parquet` están en `.gitignore`.

## Comandos

- Setup: `python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt`
- Descarga orbital: `python scripts/fetch_gp_history.py --start 2000-01-01` —
  **siempre acotar `--start`**: el archivo completo son ~12 GB y no entra en la
  partición raíz. Resume automático; `--limit 5` para pruebas; `--reset` solo
  intencional (clase "1 / lifetime").
- OMNI: `python scripts/fetch_omni.py --start 2012-01-01` (`--limit 1` para
  probar). Sin key. Ya ejecutado para 2012→2026; para 1963+ extendé el
  `--start` (el resume por año saltea lo ya bajado).
- DONKI: `python scripts/fetch_donki.py` (`--endpoints GST,SEP` para probar).
  Requiere `NASA_API_KEY`.
- Gunter's: `python scripts/fetch_gunter.py` (`--max-pages 100 --delay 2.0`
  para probar). Crawler por batches; resume-safe. Ya ejecutado (solo crawl
  completo una vez; si se re-corre, `--prune-aliases` limpia los alias
  `/doc_sdat/doc_sdat/<pag>` del sitio).
  Export tabular: `notebooks/02_gunter_tabular.ipynb` (ya ejecutado) →
  `data/gunter/gunter_tabular.parquet`.
- Credenciales: copiar `.env.example` → `.env` y completar (incluye
  `NASA_API_KEY`).
- **Atribución Gunter's obligatoria** (robots.txt permite crawlear pero la
  site se declara `noai`): derivados → "© Gunter Dirk Krebs 1996–2026,
  Gunter's Space Page (https://space.skyrocket.de)". No usar para entrenar IA.

## Convenciones (aplicar a todo script nuevo)

- Espejo de `fetch_gp_history.py`: `argparse` con `--help` descriptivo,
  `logging` (`%(asctime)s [%(levelname)s] %(message)s`), **retries con backoff
  exponencial** y **resume** vía claves estables en `data/.progress.json`.
- Salida en **Parquet particionado por año** (`year=YYYY/`), columnas
  normalizadas (datetimes en UTC, `Int64` nullable para enteros).
- Cortesía con APIs ajenas: pausa mínima entre requests, respetar rate limits,
  ventanas que se subdividen si fallan.
- Crawler de sitios web (`fetch_gunter.py`): respetar `robots.txt`, delay
  mínimo entre requests, guardar el HTML/texto crudo además de las tablas.
- **No agregar comentarios** al código salvo que se pidan.
- Cada fuente vive en su subcarpeta bajo `data/` (`data/gp_history`, después
  `data/omni`, `data/donki`, `data/gunter`).

## Decisiones registradas

- **Posición orbital exacta (SGP4)** para la Fase 3, propagando el TLE más
  cercano (±7 días) al pico de cada tormenta; el error de SGP4 crece con la
  antigüedad del TLE.
- **Event study poblacional** (efectos fijos por satélite) con Starlink
  feb-2022 como validación canónica.
- **Gunter's = capa narrativa** extra (fechas gruesas, causa en prosa): fuera
  del event study, solo validación puntual.
- Gotcha OMNI: **en OMNI no hay `Dst` ni `Kp`**. Los índices disponibles son
  `SYM_H`/`SYM_D` (corriente en anillo; `SYM_H` reemplaza a `Dst` pero **no** es
  numéricamente comparable) y `AE_INDEX`/`AL_INDEX`/`AU_INDEX`. Solo 3 canales
  de protones (10/30/60 MeV), y con **44,6% de nulos** (nulo = sin dato de
  evento, no flujo cero). Los umbrales de tormenta del §3.2 son
  `SYM_H ≤ −50 nT` **o** `AE_INDEX_max ≥ 1000 nT`.
- Gotcha OMNI: el CSV de HAPI **no trae fila de encabezado** — hay que pasar
  `header=None, names=<los nombres de /info>`. Sin eso, la primera fila de
  datos se vuelve el nombre de las columnas y el Parquet queda corrupto en
  silencio. El colector aborta si no logra enumerar `/info`.
- Gotcha OMNI: los fills de `OMNI_HRO_5MIN` son centinelas **pequeños por
  parámetro** (99.99 en `Pressure`, 99999.9 en `flow_speed`, 99999 en `SYM_H`,
  …), no `-1e31`. Se toman del campo `fill` de `HAPI /info`; un umbral `1e20`
  no los captura.
- Gotcha OMNI: `--parameters` está atado al orden de `/info`. HAPI devuelve
  solo lo pedido (hay que incluir `Time` explícitamente o no hay eje temporal) y
  rechaza subconjuntos desordenados con **error 1411 `Parameter out of order`**
  *devuelto con HTTP 200*. El colector reordena el subconjunto según `/info`.
  Además **no sobrescribe** una partición completa con un fetch de menos
  columnas sin `--reset`: sin ese guard una prueba con `--parameters SYM_H`
  borra en silencio las otras 45 columnas del año.
- Gotcha OMNI: las columnas con extremo horario llevan sufijo (`BZ_GSM_max`,
  `AE_INDEX_max`, `PR-FLX_10_max`); `SYM_H`/`SYM_D` son `min` sin sufijo. El
  mapeo es **posicional** según el orden de `/info`: un corrimiento de columnas
  no daría error visible, así que hay que revalidar contra valores conocidos
  (mínimos anuales de `SYM_H`) si se toca el dataset o el script.
- Gotcha OMNI: la cobertura del servidor llega ~24 días antes de la fecha de
  consulta (parada en 2026-09-03). Re-correr el colector completa 2026.
- Gotcha: en los parquet de `gp_history`, `DECAY_DATE` activo es `""` (no
  `NaN`) → filtrar con `.astype(str).ne("")`.
- Gotcha Gunter's: el sitio sirve **alias duplicados** `/doc_sdat/doc_sdat/<pag>.htm`
  que redirigen al canónico. El crawler los normaliza y puede limpiarse con
  `--prune-aliases`; no volver a crawlear el sitio completo (lento y
  churnoso). El export tabular es **sin clasificar**; la pérdida
  de Starlink feb-2022 no está narrada por Gunter.

## Política de datos

- Space-Track `GP_HISTORY` es clase "1 / lifetime": el archivo completo son
  ~12 GB y **no entra** en la partición raíz de Linux (56 GB, 98% lleno). Va
  acotado por fechas (`--start`) y su destino es `data/`, que debe apuntar a
  la partición de Windows. Antes de descargar: reservar el disco, no asumir
  que "ya está".
- USSPACECOM permite redistribución de datos SSA básicos citando
  `USSPACECOM/18 SDS`.
- Todos los timestamps son **UTC**.