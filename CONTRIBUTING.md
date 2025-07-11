# Contributing

To contribute to this project or run it in a local development environment, follow these steps.

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd lakebase-foreachwriter
    ```

2.  **Install Python and `uv`:**
    Ensure you have Python 3.11 or later installed. Then, install `uv`, a fast Python package installer and resolver.
    ```bash
    pip install uv
    ```

3.  **Create a virtual environment and install dependencies:**
    Use `uv` to create and sync a virtual environment. This will install all runtime and development dependencies listed in `pyproject.toml`.
    ```bash
    uv venv
    uv sync --dev
    ```
    Don't forget to activate the environment in your shell (e.g., `source .venv/bin/activate`).

4.  **Set up environment variables for testing:**
    The integration tests require a live database connection. Create a `.env` file in the root of the project and add your credentials:
    ```bash
    # .env
    LAKEBASE_USER="your-service-principal-id"
    LAKEBASE_PASSWORD="your-service-principal-secret"
    LAKEBASE_NAME="your-lakebase-instance-name"
    ```
    The tests will automatically load these variables.

5.  **Run checks and tests:**
    You can run all the checks locally using the following commands:
    ```bash
    # Check code formatting
    uv run ruff format --check .

    # Run the type checker
    uv run ty check src tests

    # Run unit tests
    uv run pytest tests/test_writer.py

    # Run integration tests (requires .env file)
    uv run pytest tests/test_integration.py
    ``` 