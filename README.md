[![](https://img.shields.io/badge/python-3.11-blue)](https://img.shields.io/badge/python-3.11-blue)

# sync-bq-postgres

Este repositorio contiene un script de Python que automatiza la actualización de datos de reglas de clientes. Extrae información de tablas en BigQuery, la procesa con Pandas y carga los datos transformados en la base de datos PostgreSQL clientes_reglas_dev.

Este script se procesa bajo demanda de Ricardo Parra cuando se genera algún nuevo registro en el mantenedor de paramétrico de reglas de corte.

[Documentacion Proyecto ](https://docs.google.com/document/d/1b5Kvv0iVXSyvlm5DiB306zcpLitl6Gkd9LbUPsQATAk/edit?usp=sharing)

## Requerimientos

Instalar Python version 3.11

En linux (Guía de ejemplo): https://python-guide-es.readthedocs.io/es/latest/starting/install3/linux.html

Instalar pip
En linux y macOs (Guía de ejemplo): https://pip.pypa.io/en/stable/installing/


## Instalacion

Para instalar dependencias

```bash
pip install -r requirements.txt
```
## Configuración

Antes de ejecutar el proyecto, asegúrate de que las credenciales de GCP para el entorno de producción estén almacenadas correctamente en la siguiente ruta:

```bash
utils\GOOGLE_APPLICATION_CREDENTIALS.JSON
```
 ## Ejecución

 Para ejecutar la sincronización manualmente, usa:

 ```
python src/main.py
```