# prueba_alten
prueba_alten

## Autenticación y GCP

El pipeline Python está diseñado para autenticarse en BigQuery mediante una Service Account utilizando una clave JSON configurada con la variable de entorno `GOOGLE_APPLICATION_CREDENTIALS`.

En este entorno, la creación de claves JSON para cuentas de servicio está deshabilitada por una política de la organización (`iam.disableServiceAccountKeyCreation`), lo que impide descargar la clave aunque la Service Account y los permisos estén correctamente configurados.

En un entorno sin esta restricción, el pipeline se ejecutaría normalmente utilizando dicha clave de Service Account, tal y como se describe en el enunciado del ejercicio.
