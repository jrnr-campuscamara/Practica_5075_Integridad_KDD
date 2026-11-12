# Informe de Calidad de Datos y Preparación KDD

**Lote analizado:** `raw_batch_202611`  
**Scripts:** `scripts/01_verificar_integridad.py` y `scripts/02_pipeline_saneamiento.py`  
**Métricas completas:** `output_samples/metricas_perfilado.json`

---

## 1. Objetivo

Construir una pasarela de calidad (*data quality gate*) que impida que datos corruptos, manipulados o incoherentes lleguen al almacén de BI. El pipeline cubre las fases de **selección, preprocesamiento y transformación** del proceso KDD, dejando el dataset listo para la fase de **minería de datos**.

| Fase KDD | Qué se hace en este pipeline |
|---|---|
| Selección | Solo entran los ficheros con firma SHA-256 válida (Fase 1) |
| Preprocesamiento | Tratamiento de nulos, fechas inválidas, reglas de dominio, outliers y duplicados (Fase 2) |
| Transformación | Tipado (`float`, `int`), fechas en ISO 8601, orden y exportación (Fase 3) |
| Minería / Interpretación | Queda para el equipo de BI a partir de `processed/dataset_kdd_listo.csv` |

## 2. Requisitos de calidad definidos (CE 3.a)

| Dimensión ISO/IEC 25012 | Requisito de negocio |
|---|---|
| Integridad | El hash SHA-256 de cada fichero debe coincidir con el manifiesto de origen |
| Completitud | `id_transaccion`, `timestamp`, `importe_eur` y `unidades` son obligatorias |
| Exactitud / Validez | `importe_eur > 0`, `unidades >= 1` y entera, y una fecha válida del año en curso que no sea futura |
| Consistencia | Importes dentro del rango estadístico normal (método IQR) |
| Unicidad | `id_transaccion` no puede repetirse |

## 3. Fase 1: Integridad criptográfica (CE 3.b)

| Fichero | Resultado | Destino |
|---|---|---|
| `transacciones_tienda_norte.csv` | Firma correcta | `staging/verified/` |
| `transacciones_tienda_este.csv` | Firma correcta | `staging/verified/` |
| `transacciones_tienda_sur.csv` | **Firma discrepante** | `quarantine/tampered/` |

- **Tasa de integridad criptográfica: 2/3 = 66,67 %**
- Hash esperado de `sur`: `0773c2cd...`; hash calculado: `1c47dff4...`. El fichero se modificó después de emitirse la firma, así que no se procesa ninguno de sus registros, aunque parezcan correctos.
- Los ficheros se **copian** (no se mueven) para que `raw_batch_202611/` siga siendo una zona en bruto inmutable y el pipeline se pueda volver a ejecutar.

## 4. Fase 2: Limpieza y coherencia (CE 3.c, CE 3.d)

De los **100 registros** de los ficheros verificados, **13 van a cuarentena** (`quarantine/bad_records.csv`, con una columna `motivo_rechazo`) y **87 son válidos**.

| Motivo de rechazo | Registros | Decisión |
|---|---|---|
| `NULO_EN_ID_TRANSACCION` | 2 | Descartar: sin identificador no se puede trazar ni deduplicar la venta |
| `NULO_EN_IMPORTE_EUR` | 1 | Descartar: el importe es la variable principal del análisis y no se inventa |
| `TIMESTAMP_INVALIDO` | 1 | Descartar: `2026-99-99 INVALID_TS` no es una fecha |
| `TIMESTAMP_FUTURO` | 1 | Descartar: `2027-08-21` es imposible para una venta ya registrada |
| `TIMESTAMP_ANTERIOR_AL_ANIO_EN_CURSO` | 1 | Descartar: `2024-02-11` no pertenece a este lote |
| `IMPORTE_NO_POSITIVO` | 2 | Descartar: importes de -34,50 € y 0,00 € |
| `UNIDADES_MENOR_QUE_1` | 2 | Descartar: 0 y -3 unidades |
| `OUTLIER_IQR_IMPORTE_EUR` | 3 | Descartar: 1480,00 €, 2750,90 € y 4120,00 € |
| **Total** | **13** | |

**Imputaciones:** 1 nulo en `categoria` y 1 en `metodo_pago` se imputaron con `"Desconocido"`. Son variables secundarias categóricas, así que la mediana no tiene sentido en ellas. Así se conserva la venta sin inventar información y el analista puede filtrar esos valores si lo necesita.

**Outliers (método IQR):** Q1 = 55,22 €, Q3 = 88,57 €, IQR = 33,36 € → rango válido **[5,18 €, 138,61 €]**. Se eligió IQR en vez de media ± 3σ porque es robusto: los propios outliers inflan la media y la desviación estándar y acaban "escondiéndose".

**Unicidad:** no se encontraron `id_transaccion` duplicados.

**Normalización:** `timestamp` en ISO 8601 (`YYYY-MM-DD HH:MM:SS`), `importe_eur` como `float` y `unidades` como `int`.

> Todos los registros rechazados en la Fase 2 proceden de `transacciones_tienda_este.csv`, que coincide con el fichero descrito como "íntegro pero con anomalías". Esto demuestra que **un hash correcto no garantiza datos correctos**: solo garantiza que el fichero no cambió durante el transporte.

## 5. Fase 3: Perfilado de calidad (CE 5.b)

| Métrica | Valor |
|---|---|
| Tasa de integridad criptográfica | **66,67 %** (2 validados / 1 rechazado) |
| Tasa de completitud (lote en bruto) | **95,00 %** (95 de 100 registros sin nulos) |
| Tasa de completitud (dataset final) | **100,00 %** |
| Tasa de validez | **87,00 %** (87 de 100) |
| Tasa de unicidad (dataset final) | **100,00 %** |

**Distribución de `importe_eur` tras la limpieza (87 registros):**

| Estadístico | Valor |
|---|---|
| Media | 70,99 € |
| Mediana | 73,44 € |
| Desviación estándar | 26,34 € |
| Percentil 25 | 53,80 € |
| Percentil 75 | 85,34 € |
| Mínimo / Máximo | 12,50 € / 124,47 € |

**Impacto de los outliers:** si los 3 outliers no se hubieran retirado (90 registros), la **media sería 161,41 €** y la **desviación estándar 528,78 €**, frente a 70,99 € y 26,34 € tras la limpieza. Tres registros de cien habrían **duplicado la media** y multiplicado la desviación por 20.

## 6. Conclusiones técnicas

1. **Rechazar antes que inventar.** Los nulos en columnas obligatorias se mandan a cuarentena en vez de imputarse. Imputar un importe o un identificador falsearía las ventas y la trazabilidad. Solo se imputan variables secundarias, con un valor neutro (`"Desconocido"`).
2. **La cuarentena no detiene el pipeline.** Cada registro rechazado se conserva con su motivo, así que el error queda auditable y recuperable, y el resto del lote sigue su curso.
3. **Dataset consistente para la minería.** Tras la limpieza, la media y la mediana casi coinciden (70,99 € frente a 73,44 €): la distribución es estable y no la arrastran valores extremos. Los modelos sensibles a la escala o a los extremos (regresión, clustering con k-means, etc.) no quedarán sesgados.
4. **Tipos y formatos homogéneos.** Fechas en ISO 8601 y campos numéricos correctamente tipados, de modo que el dataset se carga en el almacén de BI sin conversiones adicionales.
5. **Integridad y calidad son controles distintos y complementarios.** La Fase 1 bloqueó un fichero manipulado y la Fase 2 detectó anomalías en un fichero con firma correcta. Hacen falta las dos.

## 7. Limitaciones y mejoras

- El "año en curso" se calcula con la fecha de ejecución. En producción conviene usar la fecha del lote (`202611`) para que el pipeline sea reproducible.
- Los outliers se calculan sobre el conjunto global. Con más datos, sería mejor calcularlos por tienda o por categoría, porque una venta de electrónica puede ser legítimamente más cara que una de alimentación.
- Los outliers en cuarentena deberían revisarse manualmente: podrían ser ventas mayoristas reales y no errores del sensor.
