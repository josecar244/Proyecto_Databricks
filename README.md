# **✈️ Proyecto ETL Enterprise-Grade de Datos de Vuelos**

**Proyecto\_Databricks \- SmartData**

**Descripción:** Pipeline ETL *enterprise-grade* que transforma datos crudos de vuelos del 2015, implementando la **Arquitectura Medallion** (Bronze-Silver-Gold) en Azure Databricks con **Unity Catalog** para la gobernanza y **CI/CD completo** con **GitHub Actions** para garantizar la automatización del despliegue y la consistencia ACID con **Delta Lake**.

## **✨ Características Principales**

* 🔄 **ETL Automatizado** \- Pipeline completo con despliegue automático vía GitHub Actions.  
* 🏗️ **Arquitectura Medallion** \- Separación clara de capas, incluyendo la corrección explícita de tipos en las capas Silver y Golden.  
* 🔒 **Mitigación de Fallos** \- Tarea de **FALLBACK\_REVOKE** que borra los cambios realizados en el job en las diferentes capas antes del error.  
* 🚀 **CI/CD Integrado** \- Despliegue automático de *notebooks* y *workflows* en cada *push* a la rama principal.  
* ⚡ **Delta Lake** \- Garantiza transacciones ACID, *schema evolution* y *time travel* capabilities.  
* 🏛️ **Unity Catalog** \- Gobernanza centralizada y control de acceso granular sobre catálogos, esquemas y tablas.

## **🏛️ Arquitectura**

El flujo de datos sigue el patrón Medallion, garantizando la calidad progresiva de los datos.

### **Flujo de Datos**

📄 Origen CSV (Raw Data)  
↓  
🥉 Bronze Layer (Ingesta de vuelos, aeropuertos y aerolíneas)  
↓  
🥈 Silver Layer (Limpieza de datos y Tablas de Dimensión)  
↓  
🥇 Golden Layer (Agregaciones de Vuelos y KPIs)  
↓  
📊 Consumo BI (Power BI)

### **📦 Capas del Pipeline**

#### **🥉 Bronze Layer**

Propósito: Zona de aterrizaje y fuente de verdad histórica.  
Tablas: flights, airlines, airports.  
Características:

* ✅ Datos tal como vienen de origen.  
* ✅ Mínima manipulación.  
* ✅ Preservación de la historia de los datos crudos.

#### **🥈 Silver Layer**

Propósito: Creación de tablas limpias y dimensionales.  
Tablas: Tablas de hechos con datos limpios y dimensiones.  
Características:

* ✅ Datos validados y estandarizados.  
* ✅ Eliminación/gestión de nulos.  
* ✅ Preparación para el modelo dimensional.

#### **🥇 Golden Layer**

Propósito: Capa de reportes y KPIs Analytics-ready.  
Tablas:

* RPT\_RESUMEN\_VUELOS\_DIARIO: Resumen de la actividad diaria (distancia total, retrasos promedio, cancelaciones, etc.).  
* TM\_TIEMPO: Dimensión de tiempo final.  
  Características:  
* ✅ Pre-agregados y optimizados para consultas de BI.  
* ✅ Inclusión de la corrección explícita de tipos de datos.

## **📁 Estructura del Proyecto**

Proyecto\_Databricks/  
│  
├── 📂 .github/  
│   └── 📂 workflows/  
│       └── 📄 script\_Prod.yml            \# Pipeline CI/CD: Despliega y orquesta el Job WF\_ADB  
├── 📂 certificaciones/  
│   └── 📄 Certificaciones\_Databricks.txt  \# Documentación o evidencia de certificaciones.  
├── 📂 proceso/  
│   ├── 🐍 1\_raw\_to\_bronze.py            \# Tarea 2: Ingesta de datos crudos (Bronze Layer).  
│   ├── 🐍 2\_bronze\_to\_silver.py         \# Tarea 3: Limpieza y enriquecimiento (Silver Layer).  
│   └── 🐍 3\_silver\_to\_golden.py         \# Tarea 4: Agregación de KPIs y Reportes (Gold Layer).  
├── 📂 reversion/  
│   └── 🐍 Revoke.py                     \# Tarea 6: Revoca permisos (Lógica de Fallback/Mitigación de Fallos).  
├── 📂 scripts/  
│   └── 🐍 Preparacion\_Catalogo.py       \# Tarea 1: Crea Catálogo, Esquemas y configura Unity Catalog.  
├── 📂 seguridad/  
│   └── 🐍 Grants.py                     \# Tarea 5: Otorga permisos SELECT a grupos de consumo (Golden Layer).  
├── 📄 Job\_Flights\_Completo.png          \# Diagrama del Workflow de Databricks Jobs (WF\_ADB).  
└── 📄 README.md

## **🛠️ Tecnologías**

| Tecnología | Propósito |
| :---- | :---- |
| **Azure Databricks** | Plataforma unificada de datos y motor de procesamiento Spark. |
| **Unity Catalog** | Capa de gobernanza de datos, control de acceso y gestión de metadatos. |
| **Delta Lake** | Storage layer que garantiza transacciones ACID para las tablas del pipeline. |
| **GitHub Actions** | Automatización completa del flujo CI/CD. |
| **Python / PySpark** | Lenguajes primarios para la transformación y orquestación de datos. |
| **Databricks Jobs API** | Creación y ejecución programática del *workflow* en Producción. |

## **⚙️ Requisitos Previos**

* ☁️ Cuenta de Azure con acceso a Databricks.  
* 💻 Workspace de Databricks configurado (Desarrollo y Producción).  
* 🖥️ Cluster activo en el entorno de Producción.  
* 🐙 Cuenta de GitHub con permisos de administrador en el repositorio.

## **🚀 Instalación y Configuración**

El despliegue se basa en la configuración de **Secrets** de GitHub.

### **Configurar GitHub Secrets**

En tu repositorio: Settings → Secrets and variables → Actions.

| Secret Name | Propósito |
| :---- | :---- |
| DATABRICKS\_DEST\_HOST | URL del Workspace de Producción |
| DATABRICKS\_DEST\_TOKEN | Token de acceso personal (PAT) para el Workspace de Producción con permisos de Job, Cluster y Workspace. |
| CLUSTER\_ID | ID del Cluster en Producción |

## **💻 Uso (Despliegue y Ejecución)**

### **🔄 Despliegue Automático (Recomendado)**

El *workflow* **script\_Prod.yml** se activa con cualquier *push* a la rama main.

**GitHub Actions ejecutará:**

* 📤 Despliegue de todos los *notebooks* a /Proyecto\_Flights.  
* 🔧 Creación/Actualización del *workflow* WF\_ADB.  
* ▶️ Ejecución inmediata de todo el pipeline: `Preparación` **→** `Bronze` **→** `Silver` **→** `Gold`.  
* Monitoreo del Job hasta su finalización.

### **🔄 Orquestación del Workflow Databricks (WF\_ADB)**

Este Job está diseñado para ser tolerante a fallos en la capa de consumo:

| Tarea | Condición de Ejecución |
| :---- | :---- |
| **Tareas 1 a 5 (ETL \+ Grants)** | Se ejecutan en secuencia normal (Dependencias). |
| **Tarea 6 (Revoke)** | **All Done / At Least One Failed**. |

**Función de la Tarea 6 (`Revoke`):** Esta tarea de contingencia se ejecuta siempre que el Job haya finalizado (éxito o fallo). Su función es realizar una reversión completa (Rollback):

* Elimina todas las tablas creadas y llenadas por las tareas 1 a 4\.  
* Elimina las rutas de almacenamiento (archivos) asociadas a estas tablas en el Data Lake.

Esto asegura que no quede código ni datos parciales o corruptos en el entorno de Producción.

## **👤 Autor**

Jose Carlos Gonzales Espinoza

Data Engineering | Azure Databricks | Delta Lake | CI/CD