## Clone the repository
Open terminal

```bash
git clone <repo-url>
cd <project-folder>
````

## Install dependencies
From the project root:

```bash
uv sync
```

This creates a virtual environment and installs all dependencies from `uv.lock`.



Create a file named `.env` in the project root :
Or checkout the `.env-example` file for quicker setup

```
DB_USER=postgres
DB_PASSWORD=postgres
DB_HOST=localhost
DB_PORT=5432
DB_NAME=products
```

