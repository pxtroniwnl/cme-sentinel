# Proceso de extracción de datos — Gunter's Space Page

Crónica completa, paso a paso, de cómo se extrajo la capa narrativa de
CME Sentinel: el crawl de **Gunter's Space Page** y el export tabular final.
Documenta **lo que realmente se hizo** (decisiones, bugs encontrados, cifras)
para que cualquier persona pueda entender cómo nació cada artefacto de
`data/gunter/` y, si hace falta, reproducirlo.

> **Nota**: el proyecto **no** incluye clasificación ni dataset derivado de
> fallas. Lo que se extrae es la data tal cual (páginas + tablas + prosa), en
> formato tabular. Cualquier lectura/interpretación de las causas queda para
> etapas posteriores del análisis.

---

## 0. Resumen

| Artefacto | Cómo nació | Qué contiene |
|---|---|---|
| `data/gunter/pages/` + `meta/pages.parquet` | Crawler `scripts/fetch_gunter.py` | HTML/TXT crudo de cada página canónica (7.767) + mapa de crawl |
| `data/gunter/tables.parquet` | Crawler → tablas `#satlist` | Registro de lanzamientos: objeto, COSPAR, fecha, sitio, vehículo, remarks (32.323 filas) |
| `data/gunter/incidents.parquet` | Crawler → páginas `comsat_failures*` | Narrativas encabezado+texto (5.161 filas) |
| `data/gunter/gunter_tabular.parquet` (+`.csv`) | `notebooks/02_gunter_tabular.ipynb` | Tabla ancha 1 fila/objeto, sin filtrar ni clasificar (32.323 filas) |

Fuente: https://space.skyrocket.de — nada de esta data sale de telemetría.
Fecha de ejecución: 2026-09-21.

---

## 1. Origen y licencia de la fuente

Gunter's Space Page es un sitio curado por **Gunter Dirk Krebs** con el
estado y la causa de falla de cada satélite, escrito en **prosa libre**. No
hay API: hay que crawlear el HTML.

`robots.txt` permite `Allow: /`, pero el sitio declara dos condiciones de uso
que marcaron todo el proceso:

- **`noai`**: el contenido **no** puede usarse para entrenar modelos de
  aprendizaje automático.
- RAG/summarización solo con **atribución** y link al original.
- Bloque de atribución obligatorio en derivados/publicaciones:

  > Contenido © Gunter Dirk Krebs 1996–2026, Gunter's Space Page
  > (https://space.skyrocket.de). Usado bajo los términos de crawl del sitio.

`requirements.txt` incluye `beautifulsoup4` (parseo HTML) y las herramientas
de notebook; nada adicional fue necesario.

---

## 2. Fase 1 — Preparación del entorno

1. Entorno virtual con `uv` (Python 3.14.4, pandas 3.0.6, pyarrow 25.0.1).
2. `pip install -r requirements.txt`.
3. Confirmar que `data/`, `.env` y `*.parquet` están en `.gitignore`
   (lo están): la data cruda nunca se commitea.

## 3. Fase 2 — Diseño del crawler (`scripts/fetch_gunter.py`)

El crawler se escribió espejando el patrón de `fetch_gp_history.py`
(argparse `--help`, logging `%(asctime)s [%(levelname)s] %(message)s`,
retries con backoff exponencial, resume vía `data/.progress.json`,
salida Parquet). Decisiones tomadas con el usuario:

- **Alcance: solo satélites** → prefijos `/doc_sdat/`, `/doc_sat/` y el
  directorio `directories/sat`.
- **Delay mínimo de 1.0 s** entre requests (cortesía).
- Guardar además del HTML el texto plano (`.txt`) por página.

Detalles clave del crawler:

- **Resume**: `data/.progress.json` guarda `completed` (set de claves
  `gunter:page:<sha1>`) y `frontier` (cola pendiente). Al reanudar se
  reconstruye la cola desde `completed` + `frontier` sin re-bajar nada ya
  hecho.
- **Extracción de tablas**: `extract_incidents()` original buscaba
  encabezados h2/h3/dt y capturaba **cero** incidentes; se corrigió para
  leer las **tablas HTML** (`#satlist` y las de `comsat_failures*`) con
  BeautifulSoup.
- **Batch/ventanas**: el crawl se subdivide en lotes; si una ejecución se
  corta (timeout, muerte del proceso), al re-correr retoma donde quedó.

## 4. Fase 3 — Smoke test

Prueba de humo con `--max-pages 25` (solo `/doc_sdat/`): verificar que
descarga páginas, extrae tablas e incidentes, guarda HTML+TXT y escribe
`data/.progress.json`. Resultado OK → se procede al crawl completo.

## 5. Fase 4 — Crawl completo (y el bug de los alias)

### 5.1 Ejecución 1

Con `--limit` alto (tope de ~9.000 páginas) el crawl avanzó hasta el límite
configurado sin terminar.

### 5.2 Ejecución 2 — el sitio "solo" genera trabajo

Al reanudar, el crawl **no convergía**: la cola (`frontier`) seguía creciendo
con páginas nuevas que eran **alias duplicados**. Diagnóstico:

- El sitio sirve URLs del tipo `/doc_sdat/doc_sdat/<pag>.htm` que
  **redirigen al canónico** `/doc_sdat/<pag>.htm`.
- El crawler claveaba por `sha1(url bruta)` → trataba cada `doc_sdat/doc_sdat`
  como página distinta y se autoalimentaba (de 4.871 URLs en cola, **4.137
  eran alias duplicados**).

Fixes aplicados en `fetch_gunter.py`:

1. `normalize_url()` — colapsa segmentos de ruta consecutivos duplicados
   (`/doc_sdat/doc_sdat/…` → `/doc_sdat/…`).
2. Clave y almacenamiento por **`resp.url` final** (canónico post-redirect),
   no por la URL original.
3. `rebuild_frontier()` — al reanudar, descarta URLs que ya están como
   canónico en `completed`.
4. Flag **`--prune-aliases`** / `prune_aliases()` — limpia páginas alias ya
   descargadas (borra archivos crudos + filas de `tables`/`incidents`/`meta`).

### 5.3 Ejecución 3 — convergencia

Con los fixes, el crawl final **terminó con cola 0**:

> `Done: 461 pages crawled, 94 failed, 0 queued, 62499 index-table rows,
> 9089 incidents, 11711 pages on disk`

### 5.4 Limpieza de alias

Primera pasada de `--prune-aliases`: borró archivos alias y reescribió `meta`,
pero **crasheó con TypeError** (`load_existing` devolvía lista en vez de
DataFrame). Fix: envolver con `pd.DataFrame(...)`. El prune quedó
**idempotente** (segunda pasada: "No alias pages to prune"). La limpieza de
`tables.parquet` e `incidents.parquet` se completó de forma explícita.

Cifras finales tras prune:

| Artefacto | Antes | Después |
|---|---|---|
| `tables.parquet` (filas) | 62.499 | **32.323** |
| `incidents.parquet` (filas) | 9.089 | **5.161** |
| `meta/pages.parquet` (páginas canónicas) | — | **7.767** |
| Páginas en disco (pares `.html`+`.txt`) | — | ≈ 7,8k |
| `data/gunter/` tamaño | — | **231 MB** |

**Lección**: no crawlear de nuevo el sitio completo; ya está hecho una vez
(solo crawl completo una vez; un re-crawl es lento y churnoso por culpa de
los alias).

## 6. Fase 5 — Export tabular (`notebooks/02_gunter_tabular.ipynb`)

El usuario pidió la data **"tal cual"**, en formato tabular (`fecha, objeto,
razón, todas las variables posibles`), en un **notebook**, **sin filtrar ni
clasificar** nada (la data ya estaba bajada). Decisiones:

- Filas = **todas las filas del registro `#satlist` de `doc_sdat`** (una por
  objeto/lanzamiento).
- Sí extraer los metadatos del bloque HTML `#satdata`.

El notebook:

1. Lee `meta/pages.parquet` (6.710 páginas `doc_sdat`) y `tables.parquet`
   (registro de lanzamientos).
2. Parsea con BeautifulSoup cada página cruda:
   - `#satdescription` → `description_text` (prosa, "la razón").
   - `#satdata` → 11 campos: `nation, type_application, operator,
     contractors, equipment, configuration, propulsion, power, lifetime,
     mass, orbit`.
3. `mentions_years` — años (19|20)xx citados en la prosa (regex, sin inferir).
4. Salida → `data/gunter/gunter_tabular.parquet` (+ `.csv`).

**No** hay clasificación ni dataset de fallas: la interpretación de la prosa
queda para el análisis posterior.

Ejecución headless: `jupyter nbconvert --execute --inplace`. Nota técnica: el
kernel de nbconvert se lanza desde el directorio del notebook; se detecta la
raíz del repo subiendo hasta `data/gunter/meta/pages.parquet`.

Cifras: 32.323 filas de objetos · 5.152 páginas con registro · 0 páginas sin
parsear. COSPAR usan `-` para objetos planeados; `launch_date` parsea
`DD.MM.YYYY`.

## 7. Cómo re-ejecutar (runbook)

```bash
# 1. Crawl completo de Gunter's (resume-safe; ya hecho, no re-correr salvo necesidad)
python scripts/fetch_gunter.py --path-prefix doc_sdat,doc_sat --delay 1.0
#    si se re-corre, limpiar alias tras terminar:
python scripts/fetch_gunter.py --prune-aliases

# 2. Export tabular (notebook)
jupyter nbconvert --to notebook --execute --inplace notebooks/02_gunter_tabular.ipynb
#    o abrirlo con jupyter:
#    jupyter notebook notebooks/02_gunter_tabular.ipynb
```

Leer resultados:

```python
import pandas as pd
d = pd.read_parquet("data/gunter/gunter_tabular.parquet")
```

## 8. Limitaciones y advertencias

- **Prosa de una sola persona** (Gunter): fechas gruesas ("April 1997"),
  causas a veces especulativas. Es una capa **narrativa**, no dato primario
  (los datos primarios son OMNI/DONKI/Space-Track).
- `comsat_failures*` solo cubre ~pre-2004; satélites posteriores se narran en
  su página `doc_sdat`.
- No usar la data para entrenar IA (`noai`); RAG/summarización solo con
  atribución y link.
- La "fecha" precisa de la tabla es la de lanzamiento; las fechas de eventos
  van en `mentions_years` (tolerancia gruesa).

## 9. Atribución

> Todo el contenido por objeto proviene de © Gunter Dirk Krebs 1996–2026,
> Gunter's Space Page (https://space.skyrocket.de). Usado bajo los términos
> de crawl del sitio (`robots.txt`, uso con atribución y sin entrenamiento de
> modelos).