# Proyecto ETL con Airbyte + Airflow en AWS

Este proyecto implementa un pipeline de datos utilizando **Airbyte** como conector de datos y **Apache Airflow** como orquestador.  
La infraestructura se despliega en **AWS EC2** con almacenamiento en **S3**.

---

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

