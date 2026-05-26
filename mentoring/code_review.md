# Code Review: Pipeline de Entregas

## Observaciones de Código

**1. Uso de Pandas para ingesta (Cuello de botella en memoria)**
* **Qué está mal:** Se lee el archivo crudo con `pd.read_csv()` y convirtiéndolo a un DataFrame de Spark después de procesarlo.
* **Por qué importa:** Pandas carga todos los datos directamente en la memoria RAM de un solo nodo (el Driver). Si el archivo CSV es pesado, el servidor puede colapsar con un error `Out Of Memory (OOM)` y no usa el procesamiento distribuido de Spark.
* **Cómo se corrige:** Utilizar función nativa de Spark (`spark.read.csv()`)

**2. Iteración fila por fila con `iterrows()` (Rendimiento)**
* **Qué está mal:** Se está utilizando un bucle `for` de python para iterar sobre el DataFrame de Pandas y aquí aplicar lógica de negocio.
* **Por qué importa:** Iterar en Python hace que procese los datos de forma secuencial, evitando el entorno distribuido de spark y haciendo el pipeline demasiado lento.
* **Cómo se corrige:** Aplicar operaciones nativas de PySpark, como `F.when().otherwise()` y filtros nativos (`F.col().isin()`).

**3. Formato de escritura no idempotente (Parquet vs Delta)**
* **Qué está mal:** Escribir en modo overwrite apuntando al directorio raíz del país (`"/tmp/output/" + country`), esto elimina todo el historial previo almacenado allí en caso de que no existen particiones lógicas.
* **Por qué importa:** Si el pipeline falla a la mitad, los datos quedarán corruptos. Y Parquet no soporta transacciones ACID nativas.
* **Cómo se corrige:** Cambiar el formato a delta para asegurar transacciones ACID y usar `.partitionBy()`, tratando de usar algo como `replaceWhere` para tener idempotencia de las cargas sin borrar la historia.

**4. Rutas Hardcodeadas y falta de encapsulamiento (Deuda Técnica)**
* **Qué está mal:** Las rutas (`/tmp/output/` y `data.csv`) están quemadas en el código, y la llamada `process("data.csv", "GT")` se ejecuta a nivel raíz del script.
* **Por qué importa:** Hace mas difícil la reutilización del código en distintos entornos (Dev/QA/Prod), si alguien importa este archivo, el proceso se ejecutará automáticamente sin control.
* **Cómo se corrige:** Pasar las rutas como parámetros de la función (o leerlas de un YAML) y encapsular la ejecución de test dentro de un bloque `if __name__ == "__main__":`.

---

## Cómo se lo explicaría al junior

 Actualmente estás usando `Pandas` y un ciclo `for`. Aunque esto es muy común y útil cuando hacemos ejecuciones de pruebas o análisis local, en Ingeniería de Datos necesitamos que el procesamiento se distribuya en muchos servidores a la vez aprovechando las características y todo lo que nos permite el procesamiento distribuido de Spark. Al usar un bucle `for`, hace que Spark procese todo en fila en un solo servidor, desaprovechando todo su potencial. 

Te he dejado un ejemplo en el archivo `good_code.py` usando funciones nativas de PySpark con la librería `pyspark.sql.functions`. Fíjate cómo el código queda más corto y corre mucho más rápido.

**Para que investigues y lo podamos discutir si tienes alguna duda:** 
1. Échale un vistazo a la diferencia entre *Lazy Evaluation* en Spark vs. la ejecución inmediata en Pandas.
2. Revisa la documentación de la función `F.when` en PySpark, ya que ahorra usar `if-else`.
3. Busca por qué Delta Lake es más seguro que Parquet para escribir datos de forma recurrente.

Tómate el tiempo para revisarlo, prueba el nuevo código y cualquier duda que tengas sobre cómo funcionan estas funciones nativas, me avisas y lo revisamos con ejemplos reales.