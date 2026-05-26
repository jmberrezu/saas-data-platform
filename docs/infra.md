# Infraestructura como Código (Terraform)

## Estrategia de Provisionamiento para un Nuevo Tenant

Cuando se requiere agregar un país (ej. `gt` para Guatemala), el Terraform debe:
1. **Rutas Físicas (ADLS Gen2):** Crear los contenedores y directorios base en Azure Data Lake Storage para aislar los datos crudos y las diferentes capas.
2. **Aislamiento Lógico (Unity Catalog):** Crear un `Schema` específico para el tenant (`bronze_gt`, `silver_gt`, `gold_gt`) dentro del catálogo principal del entorno (`saas_prod`).
3. **Seguridad:** Otorgar permisos específicos (Grants) para que solo los usuarios que se ha decidido leer su propio Schema por tenant u otras definiciones.
4. **Gestión de Secretos:** Provisionar un *Secret Scope* en Databricks dedicado al tenant para almacenar de forma segura tokens y credenciales sin exponerlas en el código.

## Nota sobre la Implementación de IaC

Aunque comprendo a nivel arquitectónico y conceptual los pasos requeridos para el provisionamiento de infraestructura (la creación de rutas físicas, el aislamiento lógico, las políticas de seguridad y bóvedas de secretos), actualmente no cuento con la experiencia práctica con Terraform.