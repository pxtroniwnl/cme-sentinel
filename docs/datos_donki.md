# Datos extraídos — DONKI (NASA CCMC / SWPC)

Diccionario de datos y estadísticas reales de **lo que hay en `data/donki/`**
(el marco de eventos discretos de CME Sentinel). Documenta el **contenido** de
cada endpoint: esquema columna por columna, la cadena causal entre eventos,
cobertura, huecos y trampas de silenciosidad.

> Es el complemento de la sección §2.3 del [README](../README.md), que explica
> el **por qué** de la fuente. Este documento describe **qué contiene** y, sobre
> todo, **qué decisiones de implementación se tomaron y por qué**, porque varias
> no son obvias y son fuente de errores silenciosos.

---

## 1. Resumen

Siete endpoints del [webtools CCMC](https://ccmc.gsfc.nasa.gov/donki/), de
2012-01-01 a 2026-09-03, con `scripts/fetch_donki.py`. Requiere `NASA_API_KEY`
(la plantilla está en `.env.example`).

| | |
|---|---|
| Endpoints | 7 (`CME`, `CMEAnalysis`, `GST`, `FLR`, `SEP`, `IPS`, `HSS`) |
| Ventanas completadas | 105 (7 endpoints × 15 años) |
| Archivos Parquet | **103**, no 105 — ver §7 |
| Filas totales | 24.610 |
| Tamaño en disco | 6,3 MB |
| Clave de evento | `activityID` / `gstID` / `flrID` / `sepID` / `hssID` |
| Formato temporal | ISO-8601 en **UTC**, guardado como `str` |

Las filas por endpoint:

| Endpoint | Filas | Rol en la cadena CME → satélite |
|---|---:|---|
| `CME` | 9.951 | La eyección: cuándo, desde dónde, con qué cadena enlazada |
| `CMEAnalysis` | 8.643 | Geometría y **velocidad** del CME (tiempo de aviso) |
| `GST` | 186 | Tormenta geomagnética. **El nexo con OMNI** |
| `FLR` | 3.347 | Flares. Confundir flare con CME es el error clásico |
| `SEP` | 468 | Protones energéticos → **vía electrónica**, no la orbital |
| `IPS` | 1.237 | Choque en la heliosfera, antes de llegar a la Tierra |
| `HSS` | 778 | Choque en la histoesfera, ya en la magnetósfera |

---

## 2. Layout en disco

```
data/donki/<endpoint>/year=YYYY/part-00000.parquet
```

El endpoint va en el directorio y el año en la partición, así que un join con
`data/omni/year=YYYY/` es directo y cada endpoint se puede leer solo.

El resume vive en `data/.progress.json`, que tiene la forma:

```json
{ "completed": ["donki:CME:2012", "donki:CME:2013", "..."] }
```

`completed` es una **lista** de claves estables `<familia>:<endpoint>:<año>`, no
un diccionario. De ella salen las 105 ventanas DONKI y las 15 de OMNI (120 en
total).

### ⚠️ Una ventana completada no garantiza un archivo

`SEP` 2018 y 2019 están en `completed` pero **no escribieron Parquet**: la API
no devolvió ningún evento. Por eso hay 103 archivos y no 105, y solo `SEP` tiene
13 particiones en vez de 15.

La consecuencia que importa: como la clave está marcada, **re-correr el
colector no las vuelve a pedir**. Un hueco por datos vacíos queda congelado
permanentemente. Si alguna vez se necesita reintentar, hay que borrar la clave
a mano de `completed`.

La forma más fácil de no verlo es que un `glob("data/donki/SEP/year=*/part-*.parquet")`
devuelve 13 años y no 15 sin dar ningún error. Contar particiones, no asumir.

---

## 3. La cadena causal va `GST → CME`, no al revés

Este es el detalle que más cuesta adivinar, y el que decide si el aviso
anticipado funciona.

DONKI no enlaza CME→tormenta. Enlaza **tormenta→CME**: cada `GST` lista en su
`linked_activity_ids` los eventos que la causaron, y el `CME` correspondiente va
ahí dentro.

```python
cme_ids  = set(DONKI["CME"]["activityID"])
gst2cme  = {}
for gid, links in zip(DONKI["GST"]["gstID"], DONKI["GST"]["linked_activity_ids"]):
    if links is None or not hasattr(links, "__iter__"):
        continue
    for ref in links:
        if ref in cme_ids:
            gst2cme[ref] = gid        # CME -> gstID
```

De 9.951 CME, solo **138** están enlazados a una tormenta, y cubren 94 de las
186 tormentas. El resto de CMEs no produjeron una tormenta detectable, que es lo
esperable: la mayoría no van dirigidos a la Tierra.

El sentido inverso no existe: `DONKI["CME"]["linked_activity_ids"]` **nunca**
contiene un `gstID`. Un `set` de `gstID` contra la columna del CME da 0
coincidencias, y es fácil concluir que "no hay cadena causal" cuando en realidad
se está mirando el endpoint equivocado.

Ver `notebooks/03_omni_donki_explore.ipynb` §7 para la consecuencia numérica.

### ⚠️ `isinstance(links, list)` falla en silencio

Las columnas-lista de Parquet llegan como **`numpy.ndarray`**, no como `list`.
Después de un `pd.concat` sobre 15 particiones el tipo es aún menos
intuitivo. Un chequeo `isinstance(links, list)` descarta todas las filas y deja el
mapa vacío — sin error, sin warning, con un `0` en el print que parece un
resultado legítimo ("no hay enlaces causales").

Usar `hasattr(x, "__iter__")` o normalizar explícitamente. El mismo cuidado
aplica a `all_kp_times`, que se lee como `valor[0]`, no `valor.iloc[0]`.

---

## 4. Vacío, null y cadena vacía son tres cosas

DONKI no usa centinelas numéricos como OMNI. El vacío se codifica de tres
formas y todas se dan en estos datos:

| Forma | Significado | Ejemplo |
|---|---|---|
| `NaN` en la columna `_count` | La API no devolvió el array | `CME.linkedEvents_count` 81,5% nulo |
| `None` en la columna de lista | El evento no tiene cadena | 82% de los CME |
| `""` en un campo de texto | El reporte existe pero sin dato | `CME.sourceLocation` |

Un filtro `if linkedEvents_count` funciona como "¿tiene cadena?" porque `NaN`
es truthy-como y `0` es falsy, pero es frágil: hay que distinguir el `NaN` del
`0.0` explícitamente si se cuenta el número de enlaces.

Las columnas `_count` son `float64` (no `int64`) justamente por los `NaN`. Para
contar sin `NaN`: `.notna().sum()` para "¿cuántos tienen?", y
`df[col].fillna(0).astype("int64").sum()` para "¿cuántos enlaces en total?".

---

## 5. Columnas 100% nulas

Tres columnas de `CMEAnalysis` no sirven para nada en esta ventana y vienen con
dtype `object` (no `float64`), porque Parquet no puede inferir un tipo para una
columna de puros nulos:

- `tilt` — inclinación del CME. 100% nulo.
- `minorHalfWidth` — semiancho menor. 100% nulo.
- `speedMeasuredAtHeight` — altura de medición. 54,6% nulo, y con strings
  mezclados.

No son un bug del colector: el Catálogo SWPC los tiene en la interfaz web y en
`webtools`, pero la API pública `donki` no los puebla. Si el proyecto llega a
necesitar la geometría 3D de la burbuja (el objetivo de visualización del README
§6), habrá que ir a la fuente de SOHO/LASCO, no a DONKI.

`speed`, `halfAngle`, `latitude` y `longitude` sí vienen completos y son
suficientes para el cálculo de tiempo de tránsito.

---

## 6. `activeRegionNum` significa cosas distintas según el endpoint

Mismo nombre, semántica distinta:

| Endpoint | Nulos | Lectura |
|---|---:|---|
| `CME` | 79,7% | Casi ningún reporte de CME lo da |
| `FLR` | 4,4% | Casi todos los flares lo dan |

Un join `CME ↔ FLR` por `activeRegionNum` para "buscar el flare que disparó el
CME" une 20% de los CME contra algo y **pega todos los flares de la misma
región aunque no tengan nada que ver**. Para esa relación está `linkedEvents`,
que es lo que DONKI afirma, no lo que se infiere.

---

## 7. Cobertura y huecos reales

| Endpoint | Años | Filas | Nota |
|---|---|---:|---|
| `CME` | 15/15 | 9.951 | denso |
| `CMEAnalysis` | 15/15 | 8.643 | denso |
| `GST` | 15/15 | 186 | ~12/año, como corresponde |
| `FLR` | 15/15 | 3.347 | denso |
| `SEP` | 13/15 | 468 | **2018 y 2019 vacíos en la API** |
| `IPS` | 15/15 | 1.237 | denso |
| `HSS` | 15/15 | 778 | denso |

El hueco de `SEP` 2018–2019 **no es un fallo de descarga**: las ventanas están
marcadas como completadas y la API devuelve 0 eventos. El dataset público de SEP
de CCMC tiene lag de publicación. Cualquier análisis de la vía electrónica
debe tratar 2018 y 2019 como ventana ciega, no como "cero protones".

### `gstID = "none"`

Un registro de `GST` (2016-02-17) tiene el `gstID` con el **valor literal
`"none"`** en vez de un ID con formato. No es nulo, así que un filtro
`df.gstID.notna()` lo deja pasar y el ID `"none"` acaba como si fuera una
tormenta más. Es el único registro afectado, pero ensucia cualquier agrupación
por `gstID`.

### `linkedEvents` que cuelgan

369 referencias apuntan a eventos que no están en la descarga local:

| Tipo | References |
|---|---:|
| `RBE` (radio burst event) | 280 |
| `MPC` (magnetopause crossing) | 88 |
| `CME` anterior a 2012 | 1 |

- **RBE** y **MPC** no son endpoints que este proyecto descarga, así que su
  ausencia es esperada y no se puede arreglar sin ampliar la descarga.
- El `CME` de 2012-02-14 está justo fuera de la ventana `--start 2012-01-01`.
  Con un `--start` más temprano se resolvería.

Para el cruce causal, filtrar por existencia real:

```python
cme_ids = set(DONKI["CME"]["activityID"])
refs = [r for r in DONKI["CME"]["linked_activity_ids"].dropna().explode()
        if "-CME-" in r and r in cme_ids]
```

---

## 8. Validación ejecutada

- **7 endpoints × 15 años** = 105 ventanas marcadas completadas; 0 retries.
- **103 Parquet**, ninguno corrupto, ninguno de 0 bytes.
- **24.610 filas** totales; conteo por endpoint en §1.
- **`GST` × OMNI**: las 186 tormentas tienen ventana en OMNI (0 huérfanas).
  - 186/186 cumplen `SYM_H ≤ −50` o `AE_INDEX_max ≥ 1000`.
  - 171 por la rama `SYM_H`, 180 por la rama `AE`, 15 solo por `AE`.
- **Causalidad `GST` → `CME`**: 138 CME enlazados, 94 tormentas cubiertas.
- **`SEP` 2018/2019** re-consultado contra la API: 0 eventos. Vacío real.

La validación de la definición de tormenta y el contraste Kp/AE están en
`notebooks/03_omni_donki_explore.ipynb` §4.

---

## 9. Diccionario de columnas

Tipos y nulos reales, medidos sobre los 24.610 registros.

#### CME — 9,951 filas × 21 columnas

| Columna | Tipo | Nulos | Lectura |
|---|---|---|---|
| `activityID` | `str` | 0.0% | Clave primaria del evento. Formato `<inicio>-CME-###`. |
| `catalog` | `str` | 0.0% | Catálogo de origen del reporte. |
| `startTime` | `str` | 0.0% | Inicio del evento según DONKI (ISO-8601, UTC). |
| `instruments_count` | `int64` | 0.0% | Nº de instrumentos que reportan. |
| `instruments_displayName` | `object` | 0.0% | Nombres de los instrumentos (lista aplanada). |
| `instruments` | `str` | 0.0% | JSON serializado del array de instrumentos. |
| `sourceLocation` | `str` | 0.0% | Posición en la superficie solar. Viene como `""` si el reporte no la da. |
| `activeRegionNum` | `float64` | 79.7% | Número de región activa NOAA. 79,7% nulo: solo lo dan los reportes con región identificada. |
| `note` | `str` | 0.0% | Texto libre del reportador. |
| `submissionTime` | `str` | 0.0% | Cuándo DONKI recibió el reporte (puede ser años después del evento). |
| `versionId` | `int64` | 0.0% | Versión del registro. >1 = DONKI lo corrigió. |
| `link` | `str` | 0.0% | URL del evento en webtools CCMC. |
| `cmeAnalyses_count` | `int64` | 0.0% | Nº de análisis de plano de choque asociados. |
| `cmeAnalyses` | `str` | 0.0% | JSON serializado de los análisis. Gemelo exacto de la tabla `CMEAnalysis`. |
| `linkedEvents_count` | `float64` | 81.5% | Nº de eventos enlazados por DONKI. Nulo = este evento no tiene cadena. |
| `linked_activity_ids` | `object` | 81.5% | Lista de `activityID` enlazados. Permite recorrer la cadena causal. |
| `linkedEvents` | `str` | 81.5% | JSON serializado de los enlaces. |
| `sentNotifications` | `str` | 79.6% | JSON de las alertas enviadas por DONKI. Nulo = no hubo alerta. |
| `sentNotifications_count` | `float64` | 79.6% | Nº de alertas enviadas. |

#### CMEAnalysis — 8,643 filas × 24 columnas

| Columna | Tipo | Nulos | Lectura |
|---|---|---|---|
| `time21_5` | `str` | 0.0% | Hora del plano de choque a 21,5 radios. **Es la referencia temporal del análisis.** |
| `latitude` | `float64` | 0.0% | Latitud heliocéntrica del eje del CME. |
| `longitude` | `float64` | 0.0% | Longitud heliocéntrica del eje del CME. |
| `halfAngle` | `float64` | 0.0% | Ángulo medio del CME (burbuja). En grados. |
| `speed` | `float64` | 0.0% | Velocidad del CME en km/s. Insumo del cálculo de tiempo de tránsito (notebook §7). |
| `type` | `str` | 0.0% | Modelo de la burbuja: C = cone, S = sinusoid, G = gaussian. |
| `isMostAccurate` | `bool` | 0.0% | DONKI marca cuál de los varios análisis es el válido. Filtrar por `True`. |
| `associatedCMEID` | `str` | 0.0% | `activityID` del CME al que pertenece este análisis. **La clave del join.** |
| `associatedCMEstartTime` | `str` | 0.0% | Inicio del CME padre. |
| `note` | `str` | 0.0% | Texto libre del reportador. |
| `associatedCMELink` | `str` | 0.0% | URL del CME padre. |
| `catalog` | `str` | 0.0% | Catálogo de origen del reporte. |
| `featureCode` | `str` | 0.0% | Código de característica (CME-ENL, CME-CAT…). |
| `dataLevel` | `str` | 0.0% | Nivel de procesamiento del dato. |
| `measurementTechnique` | `str` | 0.0% | Técnica de medición del análisis. |
| `imageType` | `str` | 0.0% | Tipo de imagen de origen. |
| `tilt` | `object` | 100.0% | Inclinación del CME. **100% nulo**, y llega con dtype `object`. |
| `minorHalfWidth` | `object` | 100.0% | Semiancho menor del CME. **100% nulo**, y llega con dtype `object`. |
| `speedMeasuredAtHeight` | `object` | 54.6% | Altura a la que se midió la velocidad. 54,6% nulo, dtype `object`. |
| `submissionTime` | `str` | 0.0% | Cuándo DONKI recibió el reporte (puede ser años después del evento). |
| `versionId` | `int64` | 0.0% | Versión del registro. >1 = DONKI lo corrigió. |
| `link` | `str` | 0.0% | URL del evento en webtools CCMC. |

#### GST — 186 filas × 16 columnas

| Columna | Tipo | Nulos | Lectura |
|---|---|---|---|
| `gstID` | `str` | 0.0% | Clave primaria de la tormenta. **Un registro tiene el valor literal `"none"`** (2016-02-17). |
| `startTime` | `str` | 0.0% | Inicio del evento según DONKI (ISO-8601, UTC). |
| `allKpIndex_count` | `int64` | 0.0% | Nº de reportes de Kp de la tormenta. |
| `all_kp_max` | `float64` | 0.0% | Máximo de `kpIndex` en toda la tormenta. **Es el Kp del evento.** |
| `all_kp_times` | `object` | 0.0% | Marca de tiempo de cada reporte de Kp. El primero ≈ inicio de la tormenta. |
| `allKpIndex` | `str` | 0.0% | JSON serializado del array de Kp (con `source`). |
| `link` | `str` | 0.0% | URL del evento en webtools CCMC. |
| `linkedEvents_count` | `float64` | 6.5% | Nº de eventos enlazados por DONKI. Nulo = este evento no tiene cadena. |
| `linked_activity_ids` | `object` | 6.5% | Lista de `activityID` enlazados. Permite recorrer la cadena causal. |
| `linkedEvents` | `str` | 6.5% | JSON serializado de los enlaces. |
| `submissionTime` | `str` | 0.0% | Cuándo DONKI recibió el reporte (puede ser años después del evento). |
| `versionId` | `int64` | 0.0% | Versión del registro. >1 = DONKI lo corrigió. |
| `sentNotifications` | `str` | 3.2% | JSON de las alertas enviadas por DONKI. Nulo = no hubo alerta. |
| `sentNotifications_count` | `float64` | 3.2% | Nº de alertas enviadas. |

#### FLR — 3,347 filas × 22 columnas

| Columna | Tipo | Nulos | Lectura |
|---|---|---|---|
| `flrID` | `str` | 0.0% | Clave primaria del flare. |
| `catalog` | `str` | 0.0% | Catálogo de origen del reporte. |
| `instruments_count` | `int64` | 0.0% | Nº de instrumentos que reportan. |
| `instruments_displayName` | `object` | 0.0% | Nombres de los instrumentos (lista aplanada). |
| `instruments` | `str` | 0.0% | JSON serializado del array de instrumentos. |
| `beginTime` | `str` | 0.0% | Inicio del flare. |
| `peakTime` | `str` | 0.0% | Hora del máximo del flare. |
| `endTime` | `object` | 1.3% | Fin del flare. 1,3% nulo: flare aún en curso al momento de la consulta. |
| `classType` | `str` | 0.0% | Clase del flare: A, B, C, M, X + magnitud (ej. `M8.7`). |
| `sourceLocation` | `str` | 0.0% | Posición en la superficie solar. Viene como `""` si el reporte no la da. |
| `activeRegionNum` | `float64` | 4.4% | Número de región activa NOAA. Solo 4,4% nulo, a diferencia de `CME` (79,7%). |
| `note` | `str` | 0.0% | Texto libre del reportador. |
| `submissionTime` | `str` | 0.0% | Cuándo DONKI recibió el reporte (puede ser años después del evento). |
| `versionId` | `int64` | 0.0% | Versión del registro. >1 = DONKI lo corrigió. |
| `link` | `str` | 0.0% | URL del evento en webtools CCMC. |
| `linkedEvents_count` | `float64` | 60.4% | Nº de eventos enlazados por DONKI. Nulo = este evento no tiene cadena. |
| `linked_activity_ids` | `object` | 60.4% | Lista de `activityID` enlazados. Permite recorrer la cadena causal. |
| `linkedEvents` | `str` | 60.4% | JSON serializado de los enlaces. |
| `sentNotifications_count` | `float64` | 88.0% | Nº de alertas enviadas. |
| `sentNotifications` | `object` | 88.0% | JSON de las alertas enviadas por DONKI. Nulo = no hubo alerta. |

#### SEP — 468 filas × 15 columnas

| Columna | Tipo | Nulos | Lectura |
|---|---|---|---|
| `sepID` | `str` | 0.0% | Clave primaria del evento SEP. |
| `eventTime` | `str` | 0.0% | Hora del evento SEP: onset del aumento de protones. |
| `instruments_count` | `int64` | 0.0% | Nº de instrumentos que reportan. |
| `instruments_displayName` | `object` | 0.0% | Nombres de los instrumentos (lista aplanada). |
| `instruments` | `str` | 0.0% | JSON serializado del array de instrumentos. |
| `submissionTime` | `str` | 0.0% | Cuándo DONKI recibió el reporte (puede ser años después del evento). |
| `versionId` | `int64` | 0.0% | Versión del registro. >1 = DONKI lo corrigió. |
| `link` | `str` | 0.0% | URL del evento en webtools CCMC. |
| `linkedEvents_count` | `float64` | 2.8% | Nº de eventos enlazados por DONKI. Nulo = este evento no tiene cadena. |
| `linked_activity_ids` | `object` | 2.8% | Lista de `activityID` enlazados. Permite recorrer la cadena causal. |
| `linkedEvents` | `str` | 2.8% | JSON serializado de los enlaces. |
| `sentNotifications_count` | `float64` | 34.6% | Nº de alertas enviadas. |
| `sentNotifications` | `str` | 34.6% | JSON de las alertas enviadas por DONKI. Nulo = no hubo alerta. |

#### IPS — 1,237 filas × 17 columnas

| Columna | Tipo | Nulos | Lectura |
|---|---|---|---|
| `catalog` | `str` | 0.0% | Catálogo de origen del reporte. |
| `activityID` | `str` | 0.0% | Clave primaria del evento. Formato `<inicio>-IPS-###`. |
| `location` | `str` | 0.0% | Región de observación (tierra, STEREO, Voyager…). |
| `eventTime` | `str` | 0.0% | Hora de la observación del shock en la heliosfera. |
| `submissionTime` | `str` | 0.0% | Cuándo DONKI recibió el reporte (puede ser años después del evento). |
| `versionId` | `int64` | 0.0% | Versión del registro. >1 = DONKI lo corrigió. |
| `link` | `str` | 0.0% | URL del evento en webtools CCMC. |
| `instruments_count` | `int64` | 0.0% | Nº de instrumentos que reportan. |
| `instruments_displayName` | `object` | 0.0% | Nombres de los instrumentos (lista aplanada). |
| `instruments` | `str` | 0.0% | JSON serializado del array de instrumentos. |
| `linkedEvents_count` | `float64` | 19.3% | Nº de eventos enlazados por DONKI. Nulo = este evento no tiene cadena. |
| `linked_activity_ids` | `object` | 19.3% | Lista de `activityID` enlazados. Permite recorrer la cadena causal. |
| `linkedEvents` | `str` | 19.3% | JSON serializado de los enlaces. |
| `sentNotifications` | `str` | 77.2% | JSON de las alertas enviadas por DONKI. Nulo = no hubo alerta. |
| `sentNotifications_count` | `float64` | 77.2% | Nº de alertas enviadas. |

#### HSS — 778 filas × 14 columnas

| Columna | Tipo | Nulos | Lectura |
|---|---|---|---|
| `hssID` | `str` | 0.0% | Clave primaria del shock de histoesfera. |
| `eventTime` | `str` | 0.0% | Hora de la observación del shock en la histoesfera. |
| `instruments_count` | `int64` | 0.0% | Nº de instrumentos que reportan. |
| `instruments_displayName` | `object` | 0.0% | Nombres de los instrumentos (lista aplanada). |
| `instruments` | `str` | 0.0% | JSON serializado del array de instrumentos. |
| `submissionTime` | `str` | 0.0% | Cuándo DONKI recibió el reporte (puede ser años después del evento). |
| `versionId` | `int64` | 0.0% | Versión del registro. >1 = DONKI lo corrigió. |
| `link` | `str` | 0.0% | URL del evento en webtools CCMC. |
| `linkedEvents` | `str` | 53.5% | JSON serializado de los enlaces. |
| `sentNotifications` | `object` | 100.0% | JSON de las alertas enviadas por DONKI. Nulo = no hubo alerta. |
| `linkedEvents_count` | `float64` | 53.5% | Nº de eventos enlazados por DONKI. Nulo = este evento no tiene cadena. |
| `linked_activity_ids` | `object` | 53.5% | Lista de `activityID` enlazados. Permite recorrer la cadena causal. |

---

## 10. Cómo leerlo

```python
import pandas as pd
from pathlib import Path

DATA = Path("data/donki")

def leer(endpoint: str) -> pd.DataFrame:
    """Un endpoint completo, 2012-2026."""
    archivos = sorted(DATA.glob(f"{endpoint}/year=*/part-00000.parquet"))
    df = pd.concat([pd.read_parquet(f) for f in archivos], ignore_index=True)
    return df

# La CME más rápida y precisa de todo el periodo
ca = leer("CMEAnalysis")
rapida = (
    ca.query("isMostAccurate and speed > 1000")
      .assign(t=lambda d: pd.to_datetime(d.time21_5, utc=True, format="mixed"))
      .sort_values("speed", ascending=False)
)
print(f"{len(rapida):,} CMEs > 1000 km/s")

# Las tormentas, con su Kp
gst = leer("GST").sort_values("all_kp_max", ascending=False)
print(gst[["gstID", "startTime", "all_kp_max"]].head(10).to_string(index=False))
```

Dos advertencias que ya están arriba pero se repiten porque son las que
muerden:

- `linked_activity_ids` y `all_kp_times` necesitan `hasattr(x, "__iter__")`,
  no `isinstance(x, list)`.
- Las fechas son **strings ISO-8601 en UTC**, no timestamps. Convertir siempre
  con `pd.to_datetime(..., utc=True, format="mixed")`; `format="mixed"` es
  necesario porque DONKI mezcla precisión de segundos y de milisegundos entre
  endpoints.
