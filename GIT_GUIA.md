# Guía de Git para el Equipo
**Proyecto Big Data — NYC Taxi**

Esta guía explica cómo usamos Git en este proyecto. Está escrita para que cualquier miembro del equipo pueda seguirla aunque no haya usado Git antes. Léela entera una vez antes de empezar a trabajar.

---

## Por qué usamos Git

Git es un sistema que guarda el historial de cambios de todos los archivos del proyecto. Sirve para tres cosas concretas en nuestro caso:

1. **Que nadie pierda trabajo.** Si tu ordenador falla, el código está en GitHub.
2. **Que todos tengáis siempre la versión más reciente.** Sin enviar archivos por WhatsApp.
3. **Que podamos ver quién hizo qué y cuándo.** Útil si algo deja de funcionar.

---

## Estructura del repositorio

```
proyecto-bigdata/
├── data/
│   └── README_datos.md         # Instrucciones para descargar el dataset (los datos NO se suben)
├── notebooks/
│   ├── 01_exploracion.ipynb    # EDA del dataset NYC Taxi
│   ├── 02_modelo_batch.ipynb   # Entrenamiento del modelo con SparkML
│   ├── 03_streaming.ipynb      # Pipeline Spark Streaming + Kafka
│   └── 04_escalabilidad.ipynb  # Experimentos de speed-up y size-up
├── producer/
│   └── taxi_producer.py        # Script que simula el stream de viajes desde CSV
├── utils/
│   └── helpers.py              # Funciones compartidas entre notebooks
├── docs/
│   ├── memoria.pdf             # Documento escrito (10 páginas)
│   └── diapositivas.pdf        # Presentación final
├── test_kafka.py               # Script de verificación del stack
├── requirements.txt            # Dependencias Python
├── README.md                   # Instrucciones de instalación y arranque
└── GIT_GUIA.md                 # Este fichero
```

---

## Estructura de ramas

Trabajamos con dos ramas:

```
main        ← versión final y estable. Solo se toca al entregar.
develop     ← aquí trabajamos todos el día a día.
```

**Nadie trabaja en `main` nunca.** Main solo recibe cambios el día de la entrega cuando todo está revisado y funcionando.

Todo el trabajo diario va en `develop`.

---

## Configuración inicial — hacer esto una sola vez

### 1. Clonar el repositorio

```bash
git clone https://github.com/vuestro-usuario/proyecto-bigdata.git
cd proyecto-bigdata
```

### 2. Cambiar a la rama develop

```bash
git checkout develop
```

### 3. Verificar que estás en develop

```bash
git branch
```

Debe aparecer `* develop` con el asterisco delante. Si no, repite el paso 2.

---

## Flujo de trabajo diario

### Al EMPEZAR a trabajar — siempre

Antes de tocar cualquier archivo, descarga los cambios que hayan subido los demás:

```bash
git pull
```

Esto sincroniza tu copia local con lo que hay en GitHub. Si no haces esto primero y alguien ha subido cambios mientras tanto, luego tendrás conflictos innecesarios.

---

### Mientras trabajas

Trabaja con normalidad en VSCodium. Abre notebooks, edita código, ejecuta celdas. Git no interfiere mientras trabajas.

---

### Al TERMINAR una sesión — siempre

Cuando termines de trabajar, sube tus cambios en tres pasos:

**Paso 1 — Ver qué archivos has modificado:**
```bash
git status
```
Esto muestra en rojo los archivos modificados que aún no has guardado en Git. Es informativo, no hace nada.

**Paso 2 — Marcar qué quieres subir:**
```bash
git add .
```
El punto significa "todos los archivos modificados". Si solo quieres subir un archivo concreto: `git add notebooks/02_modelo_batch.ipynb`

**Paso 3 — Guardar con un mensaje descriptivo:**
```bash
git commit -m "Añadir feature engineering temporal al modelo batch"
```
El mensaje debe describir qué has hecho, no ser genérico. Ejemplos buenos y malos:

```
✅ "Añadir agregación de viajes por zona y hora"
✅ "Corregir error en lectura de Parquet enero 2009"
✅ "Completar evaluación del modelo con MAE y RMSE"

❌ "cambios"
❌ "arreglado"
❌ "trabajo"
❌ "wip"
```

**Paso 4 — Subir a GitHub:**
```bash
git push
```

---

## Resumen del flujo en una línea

```
git pull → trabajas → git add . → git commit -m "mensaje" → git push
```

Esto es el 95% de lo que usaréis en todo el proyecto.

---

## Qué hacer si hay un conflicto

Un conflicto ocurre cuando dos personas han modificado el mismo archivo y Git no sabe cuál versión es la correcta.

La señal es que `git pull` devuelve algo como:
```
CONFLICT (content): Merge conflict in notebooks/02_modelo_batch.ipynb
Automatic merge failed; fix conflicts and then commit the result.
```

**Cómo resolverlo:**

1. Avisad por WhatsApp: *"hay conflicto en el notebook 02, ¿quién tiene la versión buena?"*
2. La persona que tiene la versión correcta la sube de nuevo
3. La otra persona hace `git pull` y ya está

Los notebooks de Jupyter son especialmente propensos a conflictos porque guardan mucha metadata interna. La solución más rápida siempre es hablar y decidir cuál versión se queda.

---

## Qué NO subir a GitHub

Algunas cosas no deben estar en el repo. Para eso existe el archivo `.gitignore` que ya está configurado para excluir automáticamente:

- Los archivos de datos (`.parquet`, `.csv`) — son demasiado grandes
- Carpetas de entornos virtuales (`venv/`, `.env`)
- Archivos temporales de Jupyter (`.ipynb_checkpoints/`)
- Archivos del sistema (`.DS_Store`)

**Nunca subas los datos del dataset.** Son gigabytes. Las instrucciones para descargarlos están en `data/README_datos.md`.

---

## Comandos de consulta útiles

Ver el historial de commits:
```bash
git log --oneline
```

Ver qué cambió en un archivo concreto:
```bash
git diff notebooks/02_modelo_batch.ipynb
```

Ver en qué rama estás:
```bash
git branch
```

Deshacer cambios en un archivo antes de hacer commit (vuelve a la última versión guardada):
```bash
git checkout -- nombre_del_archivo.py
```

---

## Reglas del equipo

1. **Siempre `git pull` antes de empezar.** Sin excepciones.
2. **Nunca `git push` sin haber hecho `git pull` antes.** Genera conflictos evitables.
3. **Commits frecuentes y pequeños.** Mejor 5 commits pequeños que uno enorme al final del día.
4. **Mensajes de commit descriptivos.** En tres semanas agradecerás saber qué hizo cada commit.
5. **Los datos no van al repo.** Nunca.
6. **Si hay duda, pregunta antes de hacer force push.** El `git push --force` puede borrar el trabajo de otros.
