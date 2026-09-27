# Datos extraídos — OMNI (NASA SPDF / CDAWeb HAPI)

Diccionario de datos y estadísticas reales de **lo que hay en `data/omni/`**
(el driver continuo de CME Sentinel). Documenta el **contenido** de cada
archivo: esquema columna por columna, regla de agregación, cobertura,
distribución y caveats de calidad.

> Es el complemento de la sección §2.3 del [README](../README.md), que explica
> el **por qué** de la fuente. Este documento describe **qué contiene** y,
> sobre todo, **qué decisiones de implementación se tomaron y por qué**, porque
> varias no son obvias y son fuente de errores silenciosos.

---

## 1. Resumen

| | |
|---|---|
| Artefacto | `data/omni/year=YYYY/part-00000.parquet` |
| Particiones | 15 (años 2012 … 2026) |
| Filas totales | **128,617** (una por hora) |
| Columnas | 46 (`EPOCH` + 44 variables + `year`) |
| Tamaño en disco | ~20 MB |
| Resolver | NASA GSFC SPDF, CDAWeb HAPI (`https://cdaweb.gsfc.nasa.gov/hapi`) |
| Dataset | **`OMNI_HRO_5MIN`** — DOI `10.48322/6ffx-3441` (King & Papatashvili) |
| Licencia | Open data, CC0 — **sin key** |
| Colector | `scripts/fetch_omni.py` |

**Cobertura efectiva: 2012-01-01 → 2026-09-03.** No llega a "hoy": el endpoint
deja de servir datos ~24 días antes de la fecha de consulta (ver §6).

---

## 2. El dataset: por qué `OMNI_HRO_5MIN`

El README histórico mencionaba
`OMNI_COHO1HR_MERGED_MAG_PLASMA`. **Se descartó**: ese dataset expone solo
**12 parámetros** verificados contra `HAPI /info` — únicamente
`BR, BT, BN, ABS_B, V, elevAngle, azimuthAngle, N, T` y coordenadas
heliosféricas. **No trae ningún índice geomagnético** (ni Dst, ni Kp, ni AE):
inservible como driver de la cadena causal.

`OMNI_HRO_5MIN` expone **45 parámetros** y sí trae lo necesario:

- IMF en GSE y GSM: `BX_GSE`, `BY_GSE`, `BZ_GSE`, `BY_GSM`, `BZ_GSM`, `F`
- Viento solar: `flow_speed`, `Vx`, `Vy`, `Vz`, `proton_density`, `T`,
  `Pressure`
- Electricidad y plasma: `E`, `Beta`, `Mach_num`, `Mgs_mach_num`
- Posición GSM del bow shock: `x`, `y`, `z`, `BSN_x/y/z`
- Índices: `AE_INDEX`, `AL_INDEX`, `AU_INDEX`, `SYM_D`, `SYM_H`, `ASY_D`,
  `ASY_H`, `PC_N_INDEX`
- Protones energéticos: `PR-FLX_10`, `PR-FLX_30`, `PR-FLX_60`

---

## 3. Gotcha: el CSV de HAPI **no tiene fila de encabezado**

`/data?format=csv` devuelve **solo filas de datos**, terminando en `^M`
(CRLF), sin header. Un `pd.read_csv` ingenioso convierte **la primera fila de
datos en los nombres de columna**: el Parquet resultante tiene columnas
llamadas `-2.8`, `379`, `39645`.

Este bug existió en el script y produjo un `year=2012` corrupto antes de ser
detectado. La corrección es `header=None, names=<los nombres de /info>`, y el
script ahora **aborta con error** si no logra enumerar `/info`, en vez de
escribir basura silenciosa.

Los nombres de columna provienen de `HAPI /info` y se aplican **por posición**.
El mapeo se validó contra valores conocidos (§5).

### 3.1 `--parameters` exige el orden nativo de `/info`

Pedir un subconjunto con `--parameters` rompía de dos formas, ambas
encontradas al probar el flag:

1. **Falta el eje temporal.** HAPI devuelve *solo* lo que se pide, así que un
   subconjunto sin `Time` llega sin columna de timestamps y el resampleo falla
   con `OMNI payload has no Time column`.
2. **Error HAPI 1411 `Parameter out of order`.** El servidor exige que los
   parámetros se pidan en el **orden nativo del dataset**. Pedir
   `SYM_H,flow_speed,BZ_GSM` se rechaza porque en `/info` el orden es
   `BZ_GSM` → `flow_speed` → `SYM_H`. La respuesta es un JSON de error con
   HTTP 200, que sin validación se parsearía como si fueran datos.

El script ahora **agrega `Time` al subconjunto y reordena según `/info`**
antes de pedir, así que `--parameters` devuelve solo esas columnas (en orden
nativo) y `Time` se descarta antes de escribir el Parquet.

> ⚠️ Un fetch con `--parameters` escribe **menos columnas** que la partición
> anual completa. Por eso el colector **se niega a sobrescribir** una partición
> existente con un fetch más angosto y exige `--reset` explícito: sin ese
> guard, una prueba con `--parameters SYM_H` borra en silencio las otras 45
> columnas de ese año.

---

## 4. De 5 minutos a 1 hora: regla de agregación

OMNI es nativo en **5 minutos**. El análisis apunta a ventanas de tormenta
(horas → días), así que el colector reduce a **una fila por hora** en el
ingest. Guardar 5 min para 15 años serían ~23 M filas sin ganancia analítica.

El promedio esconde el extremo físicamente relevante en tres casos, así que la
regla es explícita y **las columnas con extremo llevan sufijo**:

| Grupo | Agregación | Sufijo | Columnas |
|---|---|---|---|
| Campos continuos | `mean` | — | `BX_GSE`, `BY_GSE`, `BZ_GSE`, `BY_GSM`, `flow_speed`, `Vx/Vy/Vz`, `proton_density`, `T`, `Pressure`, `E`, `Beta`, `Mach_num`, `Mgs_mach_num`, `x/y/z`, `BSN_x/y/z`, `percent_interp`, `Timeshift`, `RMS_SD_fld_vec`, `Time_btwn_obs` |
| Índice de corriente en anillo | `min` | — | `SYM_H`, `SYM_D`, `ASY_H`, `ASY_D` |
| Extremos de acoplamiento / SEP | `max` | `_max` | `BZ_GSM`→`BZ_GSM_max`, `AE_INDEX`→`AE_INDEX_max`, `AL_INDEX`→`AL_INDEX_max`, `AU_INDEX`→`AU_INDEX_max`, `PC_N_INDEX`→`PC_N_INDEX_max`, `PR-FLX_10/30/60`→`PR-FLX_*_max` |

Razones: `SYM_H` se cita por su **mínimo** (profundidad de la tormenta);
`BZ_GSM` por su **excursión sur** más negativa (el driver del acoplamiento);
los canales de protones por su **pico** (severidad SEP); `AE_INDEX` por su
máximo (intensidad de subtormenta).

> Nota de implementación: `Resampler.agg(**{nuevo: (col, func)})` falla con
> `ValueError: func must be a callable if args or kwargs are supplied` en
> pandas 3.x. Se usa la forma posicional `agg({col: func})` + `rename`
> posterior.

---

## 5. Fill values: el umbral `-1e31` NO alcanza

Distinto dataset, distinta convención. `OMNI_COHO1HR_MERGED_MAG_PLASMA`
usaba `-1e31`; **`OMNI_HRO_5MIN` usa centinelas pequeños por parámetro**, que
el umbral `FILL_THRESHOLD = 1e20` no captura. Antes de corregirlo, `flow_speed`
llegaba a 99999.9 km/s y `Pressure` a 99.99 nPa.

La corrección usa el valor `fill` **declarado por `HAPI /info`**, por columna:

| Fill | Columnas |
|---|---|
| `99.99` | `Pressure` |
| `99.9` | `Mach_num`, `Mgs_mach_num` |
| `99` | `IMF`, `PLS` |
| `999` | `IMF_PTS`, `PLS_PTS`, `percent_interp` |
| `999.9` | `PC_N_INDEX` |
| `999.99` | `proton_density`, `E`, `Beta`, `BZ_GSM`, `BX/BY_GSE`, `BY_GSM`, `F`, `x/y/z`, `BSN_*`, `RMS_SD_B`, `RMS_SD_fld_vec` |
| `99999` | `AE_INDEX`, `AL_INDEX`, `AU_INDEX`, `SYM_D`, `SYM_H`, `ASY_D`, `ASY_H` |
| `99999.9` | `flow_speed`, `Vx`, `Vy`, `Vz` |
| `999999` | `Timeshift`, `RMS_Timeshift`, `Time_btwn_obs` |
| `9999999.0` | `T` |
| `99999.99` | `PR-FLX_10`, `PR-FLX_30`, `PR-FLX_60` |

Los fills quedan como `NaN` tras el ingest. Se conserva además el umbral
numérico grande como red de seguridad.

---

## 6. Caveats de calidad

1. **Cobertura del servidor ≠ hoy.** El endpoint entrega datos hasta
   2026-09-03; lo posterior viene como fill `99999` y se enmascara. Re-correr
   el colector más adelante completa 2026 sin conflicto (resumen por año).
2. **`PR-FLX_*` tiene 44.6% de nulos** — los canales de protones solo se
   registran durante eventos SEP. **Un nulo ahí significa "sin dato de evento",
   no "cero flujo"**: cualquier umbral SEP debe tratar los nulos explícitamente.
3. **Huecos de viento solar ≈ 3.1%** (columnas IMF/viento solar): L1 real
   tiene gaps, más frecuentes en 2012–2014. Los índices mergeados (`SYM_H`,
   `AE_INDEX`) tienen ~0% de nulos porque OMNI los completa por modelo. Para
   análisis de acoplamiento los huecos de IMF sí importan: se recomienda usar
   `SYM_H` como índice de tormenta (completo) en vez de reconstruirlo desde Bz.
4. **`SYM_H` no es `Dst`.** Son índices distintos: `SYM_H` es el sucesor
   moderno del índice de corriente en anillo y se calcula por el método
   `SYM-H` (Welling & Vallat). Los valores publicados de `Dst` (p. ej. los
   −412 nT atribuidos a feb-2022) **no son comparables** con `SYM_H` de esta
   tabla. Para referenciar literatura hay que decirlo explícitamente.
5. **`AL_INDEX` puede salir positivo** (máx. observado +108 nT) — es una
   propiedad del producto OMNI, no un error de parseo.
6. **Mapeo posicional de columnas.** Las columnas se asignan por el orden de
   `/info`. Si el servidor devolviera las columnas en otro orden, los valores
   quedarían desplazados **sin error visible**; por eso la validación de §7
   es obligatoria tras cualquier cambio de dataset o de script.

---

## 7. Validación ejecutada

**Mapeo de columnas correcto** — mínimos anuales de `SYM_H` contra tormentas
publicadas:

| Año | `SYM_H` mín (nT) | Evento de referencia |
|---|---|---|
| 2012 | −149 | tormenta mar-2012 (Dst ≈ −150) |
| 2015 | −233 | tormenta mar-2015 (Dst ≈ −223) |
| 2017 | −144 | tormenta sep-2017 (Dst ≈ −150) |
| 2020 | −68 | mínimo solar 24 — la más débil del rango |
| 2023 | −231 | tormenta mar-2023 |
| 2024 | **−497** | tormenta Gannon may-2024, la más fuerte del ciclo 25 |

El ranking relativo (2024 la más fuerte, 2020 la más débil) coincide con el
registro histórico, lo que descarta un desplazamiento posicional de columnas.

**Rangos físicos** tras el ingest (2012–2026): `flow_speed` 250–1075 km/s,
`proton_density` 0.08–71 cm⁻³, `Pressure` 0.04–51.6 nPa, `T` 2.2e3–2.9e6 K,
`AE_INDEX_max` 6–3997 nT, `PR-FLX_10_max` hasta 5655 pfu (evento SEP mar-2012).

**Continuidad horaria**: 0 duplicados de `EPOCH`; todos los años completos
tienen exactamente 8760 u 8784 filas (bisiestos) y saltos de 1 h sin huecos.
2026 está truncado por disponibilidad del servidor.

**Caso canónico feb-2022** (la CME que perdió ~40 Starlink): el perfil
muestra la estructura de doble fase esperada — `SYM_H` cae de −3 nT a −70 nT
entre feb-3 00:00 y 09:00 UTC, `AE_INDEX_max` sube a 1445 nT, `BZ_GSM_max`
llega a −16.8 nT, con un segundo deepening los días 4–5.

---

## 8. Diccionario de columnas

45 parámetros de OMNI + `EPOCH` + `year`. Rangos sobre las 128,617 filas
horarias 2012–2026; nulos = centinelas de fill más gaps reales de L1.

| Columna | Tipo | Rango | Nulos | Lectura |
|---|---|---|---|---|
| `EPOCH` | datetime64[ns, UTC] | 2012-01-01 → 2026-09-03 | 0% | Hora UTC del sample horario. Clave temporal. |
| `SYM_H` | float64 | −497 … 69 nT | 0% | **Índice de corriente en anillo (minuto horario).** El driver de tormenta del proyecto. Sustituye a `Dst`. |
| `SYM_D` | float64 | −84 … 21 nT | 0% | Componente Y del índice de anillo. |
| `AE_INDEX_max` | float64 | 6 … 3997 nT | 0.1% | Índice auroral, máximo horario. Rol tipo-Kp ("qué tan alterada"). |
| `AL_INDEX_max` | float64 | −1473 … 108 nT | 0.1% | Componente auroralwest. Puede salir positivo. |
| `AU_INDEX_max` | float64 | −89 … 1802 nT | 0.1% | Componente auroraleast. |
| `ASY_H` / `ASY_D` | float64 | 0 … 381 / 306 nT | 0% | Activity indices asimétricos. |
| `PC_N_INDEX_max` | float64 | −5.1 … 28 nT | 11.6% | Polar cap index. |
| `BZ_GSM_max` | float64 | −39.5 … 61.8 nT | 3.1% | **IMF sur, máximo negativo horario.** Driver del acoplamiento. |
| `BZ_GSE` / `BX_GSE` / `BY_GSE` | float64 | ±47 / ±42 / ±63 nT | 3.1% | Componentes IMF en GSE. |
| `BY_GSM` | float64 | −33 … 68 nT | 3.1% | IMF transversa en GSM. |
| `F` | float64 | 0.49 … 69.1 nT | 3.1% | Módulo del IMF. |
| `RMS_SD_B` | float64 | 0 … 4.26 nT | 3.1% | Dispersión del campo. |
| `flow_speed` | float64 | 250 … 1075 km/s | 3.2% | Velocidad del viento solar (media horaria). |
| `Vx/Vy/Vz` | float64 | −1074…−250 / ±306 / ±181 km/s | 3.2% | Componentes del viento solar. Negativas en X: flujo antisolar. |
| `proton_density` | float64 | 0.08 … 70.9 cm⁻³ | 3.2% | Densidad de protones. |
| `T` | float64 | 2.2e3 … 2.9e6 K | 3.2% | Temperatura del plasma. |
| `Pressure` | float64 | 0.037 … 51.6 nPa | 3.2% | Presión dinámica. Término clave del balance de energía. |
| `E` | float64 | −60.3 … 28.7 mV/m | 3.2% | Campo eléctrico solar. |
| `Beta` | float64 | 0 … 355 | 3.2% | Beta del plasma. |
| `Mach_num` / `Mgs_mach_num` | float64 | 0.59 … 140 / 12.8 | 3.2% | Número de Mach (magnetosférico). |
| `x`, `y`, `z` | float64 | 194…266 / ±105 / ±25 Re | 3.2% | Posición del bow shock en GSM. |
| `BSN_x/y/z` | float64 | 4.1…30 / ±8.2 / ±7 Re | 3.1% | Posición del nose del bow shock. |
| `IMF`, `PLS` | float64 | 51 … 71 | 3.1% | Códigos de fuente (flags). No usar como magnitud. |
| `IMF_PTS`, `PLS_PTS` | float64 | 1 … 5 | 3.1% | Nº de puntos incluidos en la media. Proxy de calidad. |
| `percent_interp` | float64 | 0 … 100 % | 3.1% | Fracción interpolada. Proxy de calidad. |
| `Timeshift` | float64 | −6590 … 10730 s | 3.1% | Time-shift L1 → bow shock. |
| `RMS_Timeshift` | float64 | 0 … 2200 s | 3.1% | Dispersión del time-shift. |
| `Time_btwn_obs` | float64 | −1391 … 2638 s | 3.1% | Tiempo entre observaciones. |
| `PR-FLX_10_max` | float64 | 0.09 … 5655 pfu | **44.6%** | Flujo de protones >10 MeV, pico horario. Vía electrónica/SEP. |
| `PR-FLX_30_max` | float64 | 0.05 … 712 pfu | **44.6%** | Flujo >30 MeV. |
| `PR-FLX_60_max` | float64 | 0.03 … 182 pfu | **44.6%** | Flujo >60 MeV. |
| `year` | int64 | 2012 … 2026 | 0% | Partición de la tabla. |

---

## 9. Cómo leerlo

```python
import glob
import pandas as pd

# Un año
omni = pd.read_parquet("data/omni/year=2022/part-00000.parquet")

# El rango completo (~20 MB en RAM)
omni = pd.concat(
    [pd.read_parquet(f) for f in sorted(glob.glob("data/omni/year=*/part-00000.parquet"))],
    ignore_index=True,
)

# Tormentas: umbral de corriente en anillo, sin Kp (no existe en OMNI)
storms = omni[omni["SYM_H"] <= -50].copy()
```

> ⚠️ Kp **no está** en OMNI. El README §3.2 usaba
> `DST ≤ −50` **o** `Kp ≥ 5`; la definición vigente es
> `SYM_H ≤ −50 nT` (tormenta magnética) **o** `AE_INDEX_max ≥ 1000 nT`
> (equivalente razonable a G2/G3). Si se necesita Kp real, hay que
> incorporarlo de otra fuente (p. ej. GFZ Potsdam) — no está en `data/omni`.
