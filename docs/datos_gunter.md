# Datos extraídos — Gunter's Space Page

Diccionario de datos y estadísticas reales de **lo que hay en `data/gunter/`**
(la capa narrativa de CME Sentinel). Documenta el **contenido** de cada
artefacto: esquema, columna por columna, distribución, valores típicos y
caveats de calidad.

> Es complemento de [`proceso_extraccion_gunter.md`](proceso_extraccion_gunter.md),
> que narra el **proceso** (cómo se extrajo). Este documento describe **qué
> contiene**. El detalle de cómo se combina con el resto del pipeline está en el
> README (§2.5, §3).
>
> **Nota**: el proyecto **no** incluye clasificación ni dataset derivado de
> fallas. Lo que se extrae es la data tal cual (páginas + tablas + prosa), en
> formato tabular. Cualquier lectura/interpretación de las causas queda para
> etapas posteriores del análisis.

---

## 1. Resumen

Fuente: https://space.skyrocket.de (Gunter Dirk Krebs). Crawl completado el
**2026-09-21 → 2026-09-22**. Total en disco: **305 MB**.

| Artefacto | Filas | Columnas | Contenido | Origen |
|---|---|---|---|---|
| `data/gunter/tables.parquet` | 32.323 | 8 | Registro de lanzamientos extraído de las tablas `#satlist` | `scripts/fetch_gunter.py` |
| `data/gunter/incidents.parquet` | 5.161 | 4 | Texto por satélite: prosa `#satdescription` (5.153) + fallas `comsat_failures` (8) | `scripts/fetch_gunter.py` |
| `data/gunter/gunter_tabular.parquet` | 32.323 | 23 | Tabla ancha 1 fila/objeto: registro + metadatos + prosa completa, **sin filtrar ni clasificar** | `notebooks/02_gunter_tabular.ipynb` |
| `data/gunter/gunter_tabular.csv` | 32.323 | 23 | Mismo contenido que el parquet, en CSV (69 MB) | `notebooks/02_gunter_tabular.ipynb` |
| `data/gunter/meta/pages.parquet` | 7.767 | 7 | Mapa de crawl: páginas canónicas descargadas | `scripts/fetch_gunter.py` |
| `data/gunter/pages/` | 15.628 archivos | — | HTML + TXT crudos (7.814 pares `.html`/`.txt`) | `scripts/fetch_gunter.py` |
| `data/gunter/crawl*.log` | 3 archivos | — | Logs de las 3 ejecuciones del crawl | `scripts/fetch_gunter.py` |

- `tables.parquet` y `gunter_tabular.parquet` comparten las mismas 32.323
  filas a partir de la misma fuente (`#satlist`); `tables.parquet` es la
  extracción cruda del crawler y `gunter_tabular.parquet` el export
  enriquecido del notebook.
- 47 pares `.html`/`.txt` en `pages/` **no** están referenciados en
  `meta/pages.parquet` (restos de páginas alias prunedas que quedaron sin
  borrar); no afectan al análisis.

### Atribución y licencia

> Todo el contenido por objeto proviene de © Gunter Dirk Krebs 1996–2026,
> Gunter's Space Page (https://space.skyrocket.de). Usado bajo los términos
> de crawl del sitio (`robots.txt`): el contenido declara `noai`, **no** puede
> usarse para entrenar modelos; RAG/summarización solo con atribución y link.

---

## 2. `tables.parquet` — registro de lanzamientos (32.323 × 8)

Extracción cruda del crawler: una fila por objeto/lanzamiento tal como aparece
en las tablas `#satlist` de las páginas `/doc_sdat/*`. Tipos: todas las
columnas son strings (la fecha y la data numérica llegan en texto).

| Columna | No vacías / 32.323 | Únicos | Significado |
|---|---|---|---|
| `Satellite` | 32.323 | 32.056 | Nombre del objeto, p. ej. `Mars Telecommunications Orbiter (MTO)`. Puede repetirse (varias líneas/lanzamientos con el mismo nombre). |
| `COSPAR` | 32.323 | 27.743 | Designador internacional. `'-'` cuando no existe designador (3.467 filas); `2025-292` cuando hay año+número pero sin letra de designación. |
| `Date` | 32.213 | 6.440 | Fecha de lanzamiento **en texto libre**: `10.12.2025`, `2025` o solo `2028`. 110 vacías. No es un timestamp. |
| `LS` | 30.233 | 293 | Código de sitio de lanzamiento, p. ej. `CC`, `Jq LP-43/130` (la lista de códigos es del propio sitio). |
| *(sin nombre)* | 1.155 | 19 | Columna de flags sin encabezado en el sitio. Valores vistos: `F` (665), `P` (178), `*` (145), `p`, `*F`, `f`, `ND`, `*P`, `F%`. **No se interpreta ni mapea aquí** (se guarda tal cual). |
| `Launch Vehicle` | 30.252 | 736 | Vehículo de lanzamiento, p. ej. `Lijian-1 (Kinetica-1)`, `CZ-6C`. |
| `Remarks` | 26.233 | 6.537 | Observaciones del lanzamiento (p. ej. acompañantes `with ... , Jilin-1 ...`). Texto con saltos de línea y comas internas. |
| `source_url` | 32.323 | 5.152 | URL canónica de la página `doc_sdat` de procedencia. |

Caveats: `Date` no es uniforme y `COSPAR='-'` no es un dato real; ambos se
resuelven mejor con `launch_date`/`cospar` de `gunter_tabular.parquet`.

---

## 3. `incidents.parquet` — textos por satélite (5.161 × 4)

Extraído de las páginas `comsat_failures*` y de los textos de las páginas
`doc_sdat`. Una fila por (satélite, bloque de texto).

| Columna | No vacías / 5.161 | Únicos | Significado |
|---|---|---|---|
| `satellite` | 5.161 | 5.145 | Nombre del satélite al que se refiere el texto. |
| `text` | 5.161 | 5.151 | Prosa completa (párrafos con `\r\n`), longitud media ≈ 1.314 caracteres. |
| `source_url` | 5.161 | 5.154 | URL de la página de donde salió el texto. |
| `kind` | 5.161 | 2 | `description` (5.153) = bloque `#satdescription` de una página `doc_sdat`; `comsat_failure` (8) = entrada de la página `comsat_failures.htm`. |

Distribución por `kind`:

| kind | Filas | Fuente |
|---|---|---|
| `description` | 5.153 | páginas `doc_sdat/*` (reutiliza la prosa de `description_text`) |
| `comsat_failure` | 8 | `doc_sat/comsat_failures.htm` |

Los 8 `comsat_failure` son: DirecTV-1, DirecTV-3, DirecTV-6, EchoStar-3,
EchoStar-4, EchoStar-5, EchoStar-6, Nimiq-2. La página `comsat_failures*` solo
cubre ~pre-2004; satélites posteriores se narran vía su propia página
`doc_sdat` (para leer las causas de fallas modernas conviene buscar en
`description_text` de `gunter_tabular.parquet`, no aquí).

---

## 4. `gunter_tabular.parquet` — export tabular (32.323 × 23)

Tabla ancha que une el registro `#satlist` con los metadatos `#satdata` y la
prosa `#satdescription` de cada página `doc_sdat`. **Sin filtrar ni
clasificar.** Es la pieza principal para el join futuro con `gp_history` vía
`cospar` + `launch_date`.

### 4.1 Identificación y registro

| Columna | Tipo | No vacías (de 32.323) | Únicos | Significado / ejemplos |
|---|---|---|---|---|
| `satellite` | string | 32.323 | 32.056 | Nombre del objeto. |
| `cospar` | string | 32.323 | 27.743 | Designador internacional. `'-'` en 3.467 filas (sin designador); `2025-292` (año+número sin letra) en 1.836; patrón completo `YYYY-NNNL` en 27.020. |
| `launch_date` | `datetime64[us]` | 29.354 | 6.410 | Fecha de lanzamiento parseada (`DD.MM.YYYY`). Rango **1957-10-04 → 2040-01-01** (las "fechas" futuras y `2028-01-01` son lanzamientos planeados). 2.969 nulos. |
| `launch_date_raw` | string | 32.213 | 6.440 | Texto original del registro (antes del parseo). Valores no parseables: `202x` (1.430), `cancelled` (951), `20xx` (149), `not launched` (132), `on hold` (57), `Option`, `not flown`, `converted`, etc. |
| `launch_site` | string | 30.233 | 293 | Código de sitio de lanzamiento. |
| `launch_vehicle` | string | 30.252 | 736 | Vehículo de lanzamiento. |
| `remarks` | string | 26.233 | 6.537 | Observaciones del lanzamiento. |
| `source_url` | string | 32.323 | 5.152 | URL canónica `doc_sdat` de procedencia. |
| `page_id` | string | 32.323 | 5.152 | Hash (sha1 corto) de la página en `data/gunter/pages/<page_id>.html`. |
| `title` | string | 32.323 | 5.136 | Título de la página (≈ nombre del objeto principal de la página; varias filas pueden compartir página). |

### 4.2 Prosa y metadatos de la página (`#satdata` / `#satdescription`)

| Columna | No vacías | Únicos | Significado / ejemplos |
|---|---|---|---|
| `description_text` | 32.323 | 5.142 | **Prosa completa de la página** ("la razón", el estado, la historia del satélite). Longitud media ≈ 1.702 caracteres; máx. 29.727. Varias filas comparten página → mismo texto. |
| `nation` | 32.323 | 253 | País/organización. Top: `USA` (20.845), `USSR` (2.274), `China` (2.080), `USSR / Russia` (1.082), `Russia` (974), `UK (Channel Islands)` (689), `Germany` (528), `Japan` (495). |
| `type_application` | 31.911 | 759 | Aplicación/misión. Top: `Communication` (15.944), `Technology` (2.501), `Earth observation` (1.207), `Experimental Communication` (941), `Reconnaissance, photo (film return type)` (893), `Navigation` (738), `Military Communication` (575), `Communication M2M/IoT` (377). |
| `operator` | 29.056 | 1.865 | Operador, p. ej. `NASA`. |
| `contractors` | 30.506 | 1.876 | Contratistas, p. ej. `Blue Origin`. |
| `equipment` | 22.336 | 1.593 | Equipo/payload principal, p. ej. `X-band SAR`, `X-band SAR`. |
| `configuration` | 20.467 | 965 | Plataforma/bus, p. ej. `Blue Ring bus`, `CubeSat (1U)`. |
| `propulsion` | 25.831 | 523 | Propulsión, p. ej. `None`, `?` (desconocida). |
| `power` | 31.815 | 183 | Potencia/paneles, p. ej. `2 deployable solar arrays, batteries`, `Solar cells, batteries`. |
| `lifetime` | 7.492 | 482 | Vida nominal, p. ej. `5 years`. Columna muy incompleta. |
| `mass` | 26.117 | 1.663 | Masa **en texto** (p. ej. `1 kg`, `360 kg`), no numérico. |
| `orbit` | 23.841 | 2.002 | Órbita descrita en texto, p. ej. `Heliocentric, then Mars orbit`, `interplanetary, lunar orbit`. |

### 4.3 Años mencionados

| Columna | Tipo | Completitud | Significado |
|---|---|---|---|
| `mentions_years` | `list<int64>` | 17.357 con ≥1 año; 14.966 vacías | Años `(19|20)xx` citados en la prosa, extraídos por regex **sin inferir nada**. Máx. 15 años por fila. Es la única pista temporal gruesa de eventos (p. ej. año de un evento de falla), con tolerancia amplia. |

Caveats de la fuente: la "fecha" precisa del registro es la de lanzamiento
(`launch_date`); las fechas de eventos posteriores solo están como prosa y
como años en `mentions_years`. `mass`, `power`, `orbit`, `lifetime` son
**texto libre** de un único autor, no campos numéricos normalizados.

---

## 5. `meta/pages.parquet` — mapa de crawl (7.767 × 7)

Una fila por **página canónica** descargada (las URLs alias
`/doc_sdat/doc_sdat/...` quedaron fuera tras `--prune-aliases`).

| Columna | Tipo | Completitud | Significado |
|---|---|---|---|
| `url` | string | 7.767 | URL canónica final (post-redirect). Prefijos: `/doc_sdat` (6.710), `/directories` (461), `/doc_lau_det` (444), `/doc_sat` (142), `/doc_lau_fam` (6), `/doc_lau` (3), `/index.html` (1). |
| `page_id` | string | 7.767 | Hash corto; nombre del archivo crudo en `pages/<page_id>.html|txt`. |
| `title` | string | 7.767 | Título `<title>` de la página. |
| `has_index_table` | bool | 7.767 | La página tiene tabla `#satlist` → alimenta `tables.parquet`. `True` en 5.152. |
| `has_incident` | bool | 7.767 | La página tiene bloque de texto extraído → alimenta `incidents.parquet`. `True` en 5.154. |
| `size_bytes` | int64 | 7.767 | Tamaño del HTML crudo. Total ≈ 170 MB. |
| `fetched_at` | datetime UTC | 7.767 | Cuándo se descargó (2026-09-21 22:44 → 2026-09-22 03:48). |

- 5.152 páginas tienen ambas flags (`doc_sdat` con registro y texto): son las
  que originan cada fila de `gunter_tabular`.
- Las 5.152 páginas `doc_sdat` con `has_index_table=True` = las 5.152 URLs
  distintas en `tables.parquet` y `gunter_tabular`.

---

## 6. `pages/` — HTML y TXT crudos

- **15.628 archivos** = 7.814 pares `<hash>.html` + `<hash>.txt` (226 MB).
- Cada texto `.txt` es el parseo a texto plano del HTML (para lectura sin
  re-procesar).
- `meta/pages.parquet` cubre 7.767 de esos pares; los **47 restantes** son
  huérfanos de alias prunedos (no están en el mapa).
- Sirven para re-parsear con BeautifulSoup sin volver a crawlear (el parseo
  del notebook usa estos archivos, no el sitio web).

Además, `data/gunter/crawl.log`, `crawl2.log` y `crawl3.log` son los logs de
las 3 ejecuciones del crawl (ver `proceso_extraccion_gunter.md`).

---

## 7. Cómo leer

```python
import pandas as pd

# Registro de lanzamientos (crudo del crawler)
t = pd.read_parquet("data/gunter/tables.parquet")

# Textos por satélite: fallas clásicas vs prosa de descripción
inc = pd.read_parquet("data/gunter/incidents.parquet")
fallas = inc[inc.kind == "comsat_failure"]

# Export tabular: 1 fila por objeto, sin clasificar
g = pd.read_parquet("data/gunter/gunter_tabular.parquet")
g[g.cospar == "1995-025A"]                                   # un objeto por COSPAR
g.dropna(subset=["launch_date"])                             # descarta `202x`/`cancelled`/etc.
# Join futuro con gp_history: `cospar` ↔ OBJECT_ID + `launch_date` (caveat: cospar='-' sin designador)
```

---

## 8. Limitaciones y usos previstos

- **Prosa de una sola persona** (G. Krebs): fechas gruesas, causas a veces
  especulativas. Capa **narrativa** de validación puntual — **nunca** como
  estadística poblacional ni dentro del event study (§3 del README).
- Sin clasificación: la interpretación de causa queda en prosa/`remarks`;
  este repo **no** produce un dataset de fallas.
- `cospar='-'` (3.467 filas) y `launch_date` nulo (2.969) marcan objetos
  planeados/cancelados sin designador → no casables con `gp_history`.
- `comsat_failures*` solo cubre ~pre-2004; para fallas modernas hay que leer
  `description_text` (la pérdida de Starlink feb-2022 **no** está narrada).
- Uso: validación puntual del §3 (narrativa tipo Galaxy 15 / SkyTerra 1),
  ilustración de casos, y capa narrativa en la visualización 3D (§6.3).