
## Instalación

1. Instala las dependencias:
```bash
pip install -r requirements.txt
```

2. Configura las variables de entorno creando un archivo `.env` en el directorio raíz:
```env
db_host=aws-1-us-east-2.pooler.supabase.com
db_user=tu_usuario
db_password=tu_contraseña
db_port=6543
db_name=afg
```

## Configuración de PostgreSQL

### Acceso a la Base de Datos

#### 1. Conectar usando psql (línea de comandos)
```bash

psql -h aws-1-us-east-2.pooler.supabase.com -p 6543 -U tu_usuario -d afg
```

#### 2. Conectar usando pgAdmin
2. Crear una nueva conexión servidor:
   - **Host**: aws-1-us-east-2.pooler.supabase.com
   - **Puerto**: 6543
   - **Usuario**: tu_usuario
   - **Contraseña**: tu_contraseña
   - **Base de datos**: afg

#### 3. String de Conexión
El proyecto utiliza SQLAlchemy con el siguiente formato:
```bash
   postgresql+psycopg2://usuario:contraseña@host:puerto/base_datos
```

### Verificar Tablas Creadas

Después de ejecutar el script de ingesta, puedes verificar las tablas:

```sql
-- Listar todas las tablas
\dt

-- Ver estructura de la tabla atenciones_urgencias
\d atenciones_urgencias

-- Consultar datos
SELECT * FROM atenciones_urgencias LIMIT 10;
```

## Uso

### Ejecutar Ingesta de Datos

```bash
python ingestion.py
```

Este script:
1. Carga los datos desde los modulos en formato DataFrame
2. Se conecta a PostgreSQL usando las variables de entorno
3. Crea/reemplaza la tablas de destino
4. Inserta los datos procesados

