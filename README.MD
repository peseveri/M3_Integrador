# Proyecto ETL con Airbyte + Airflow en AWS

Este proyecto implementa un pipeline de datos utilizando **Airbyte** como conector de datos, **Pyspark** como herramienta de procesamiento y transformacion y **Apache Airflow** como orquestador.  
La infraestructura se despliega en **AWS EC2** con almacenamiento en **S3**.

---

# 📌 Justificación de la arquitectura y herramientas

Este proyecto ETL utiliza **Airbyte + Airflow en AWS** por los siguientes motivos:

---

## 1. Airbyte como conector de datos
- Open-source con más de **300 conectores listos** para usar.  
- Permite **extraer datos desde APIs, bases de datos y SaaS** sin desarrollar conectores desde cero.  
- Integración nativa con **S3** y posibilidad de crear conectores personalizados.  
- Comunidad activa y en constante evolución → menor riesgo tecnológico.  

---

## 2. PySpark — Transformación en batch
- transforma a escala (limpieza, normalización, joins, enrichment).
- Motor **distribuido** y **columna** orientado a grandes volúmenes.
- Lectura/escritura eficientes en **S3** (parquet/orc, particionado y compresión).
- **UDFs**, manejo de **semiestructurados** (JSON), **schema enforcement** y **optimización** (Catalyst, Tungsten).
- **Ejecución:** como **job containerizado** (driver local) dentro del stack Docker de la EC2.

## 3. Apache Airflow como orquestador
- Estándar de facto en **orquestación de pipelines**.  
- Workflows definidos como **código (DAGs)** → fácil versionado y mantenimiento.  
- Integra operadores de Airbyte y controla **dependencias, reintentos y scheduling**.  
- Escalable: puede crecer desde una sola instancia en EC2 hasta un clúster distribuido.  

---

## 4. AWS EC2 para despliegue
- Brinda **control total del ambiente** y flexibilidad en el escalado.  
- Permite instalar Airbyte y Airflow con Docker en un único servidor.  
- Configuración simple para exponer el **webserver de Airflow** mediante Security Groups.  
- Fácil migración futura a **ECS o EKS** si se requiere mayor escalabilidad.  

---

## 5. Amazon S3 como almacenamiento
- **Escalable y económico**, pagando solo por lo que se usa.  
- Alta **durabilidad y disponibilidad** 
- Separación clara entre:  
  - *Source bucket*: JSON originales de Airbyte.  
  - *Destination bucket*: datos transformados/listos.  
- Integración con otros servicios de AWS (Glue, Athena, Redshift, etc.).  

---

## 6. Docker + docker-compose
- Facilita el **despliegue reproducible y portátil**.  
- Evita conflictos de dependencias entre Airbyte y Airflow.  
- Permite levantar todo el stack con un solo comando:  
  ```bash
  docker-compose up -d

---

## 7. Gestión de credenciales con .env
- Mantiene seguridad al no exponer secretos en el código.
- Configurable por ambiente (dev, stage, prod).
- Facilita la automatización de despliegues y la colaboración en equipo.

---

En conclusión, esta arquitectura combina simplicidad inicial, flexibilidad operativa y escalabilidad futura, aprovechando la potencia de Airbyte (ingesta), Airflow (orquestación), S3 (almacenamiento) y la portabilidad de Docker.

## 🚀 Requisitos previos

Antes de comenzar, asegúrate de contar con:

- Una cuenta de **AWS** con permisos para crear recursos (EC2, S3, IAM).
- Un par de claves **AWS Access Key** y **Secret Key**.
- Tener configurado un bucket S3 para:
  - **Source:** Donde Airbyte guarda los `json` de origen.
  - **Destination:** Donde Airbyte guarda los datos ingeridos.
- Crear una **instancia EC2** (Ubuntu recomendado).
- Configurar un **Security Group** en la EC2 que permita acceso al puerto del webserver de Airflow (por defecto `8080`).

---

## ⚙️ Configuración inicial de la instancia EC2 y las dependencias

1. Conéctate a tu instancia EC2:
  ```bash
  ssh -i tu_clave.pem ubuntu@<EC2_PUBLIC_IP>
  ```
2. Instala Docker y Git:
  ```bash
  sudo apt update
  sudo apt install -y docker.io docker-compose git
  sudo usermod -aG docker $USER
  ```
3. Cierra sesión y vuelve a entrar para aplicar los permisos de Docker.

4. Clonar el repositorio
  ```bash
  git clone <URL_DE_TU_REPO>
  cd <NOMBRE_DEL_REPO>
  ```
5. Configuracion de variables de entorno
  Crea un archivo .env en la raíz del proyecto con el siguiente contenido:
  ```bash
  BASE_URL=
  CLIENT_ID=
  CLIENT_SECRET=
  AWS_ACCESS_KEY_ID=
  AWS_SECRET_ACCESS_KEY=
  AWS_DEFAULT_REGION=
  ```
  BASE_URL, CLIENT_ID y CLIENT_SECRET corresponden a la API que de airbyte cloud (ir a la cloud de airbyte y crear una aplicacion, alli se disponibilizaran las credenciales)

  Las credenciales de AWS permiten que Airbyte y Airflow accedan a S3.

6. Levantar Airbyte y Airflow
  ```bash
  docker-compose up -d
  ```
7. Accede al webserver de Airflow desde tu navegador

  http://<EC2_PUBLIC_IP>:8080

  (Asegúrate de haber abierto el puerto 8080 en el Security Group de tu EC2).


