import os
from prefect_snowflake.credentials import SnowflakeCredentials
snowflack_credintials_block=SnowflakeCredentials.load("my-snowflake-creds")
from prefect_dbt.cli.commands import DbtCoreOperation  # Module not installed, commented out
from dotenv import load_dotenv
from prefect import flow, get_run_logger
import logging
# from connectors import CONNECTORS  # Module not found, commented out


# # Setup logger
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)


#load environment variables
load_dotenv()

# Get the paths to dbt project and profiles directories from environment variables
profiles_dir = os.getenv("DBT_PROFILES_DIR")
project_dir = os.getenv("DBT_PROJECT_DIR")


def get_dbt_models():
    """
    Returns a list of dbt model file names (without .sql extension)
    from the 'models' directory in the dbt project.
    """
    dbt_models = []
    models_path = os.path.join(project_dir, "models")

    if not os.path.exists(models_path):
        raise ValueError(f"Models directory not found at: {models_path}")

    for root, _, files in os.walk(models_path):
        for file in files:
            if file.endswith(".sql"):
                # Remove extension to get model name
                model_name = os.path.splitext(file)[0]
                if model_name not in dbt_models:
                    dbt_models.append(model_name)
    return dbt_models

# if __name__ == "__main__":
#     #check models list
#     try:
#         dbt_models = get_dbt_models()
#         print("DBT Models found:")
#         print(dbt_models)
#     except Exception as e:
#         print(f"Error: {e}")




def fetch_snowflake_credentials(logger) -> dict:
    """
    Fetch Snowflake credentials from Prefect block.
    """
    try:
        snowflake_block = SnowflakeCredentials.load("my-snowflake-creds")

        creds = {
            "account": snowflake_block.account,
            "user": snowflake_block.user,
            "password": snowflake_block.password.get_secret_value(),
            "authenticator": snowflake_block.authenticator,
            "role": snowflake_block.role,
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "database": os.getenv("SNOWFLAKE_DATABASE"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA"),
        }

        logger.info(" Snowflake credentials loaded successfully.")
        return creds

    except Exception as e:
        logger.error(f" Failed to load Snowflake credentials: {e}")
        raise e
 
# @flow
# def test_fetch_snowflake_credentials():
#     logger = get_run_logger()
#     creds = fetch_snowflake_credentials(logger)
#     print(" Credentials:", creds)

# test_fetch_snowflake_credentials()


# #test snowflack credintialals
# if __name__ == "__main__":
#     try:
#         creds = fetch_snowflake_credentials(logger)
#         print("Snowflake credentials:")
#         for key, value in creds.items():
#             print(f"{key}: {value}")
#     except Exception as e:
#         print("Error:", e)


def set_env_vars(model: str):
    """
    Sets environment variables for dbt commands using Snowflake credentials.

    Args:
        model (str): The name of the dbt model, used for schema.
    """
    logger = get_run_logger()

    # Determine the environment prefix based on DBT_ENV_SECRET_TARGET
    target_env = os.getenv("DBT_ENV_SECRET_TARGET")
    if target_env == "dev":
        env_prefix = "DEV"
    elif target_env == "stage":
        env_prefix = "STAGE"
    elif target_env == "production":
        env_prefix = "PROD"
    else:
        raise ValueError(" Invalid target environment provided.")

    # Fetch Snowflake credentials from Prefect block
    creds = fetch_snowflake_credentials(logger)

    # Set environment variables for dbt to use, converting None to empty string
    os.environ[f"{env_prefix}_DBT_ENV_SECRET_ACCOUNT"] = str(creds.get("account") or "")
    os.environ[f"{env_prefix}_DBT_ENV_SECRET_USER"] = str(creds.get("user") or "")
    os.environ[f"{env_prefix}_DBT_ENV_SECRET_PASSWORD"] = str(creds.get("password") or "")
    os.environ[f"{env_prefix}_DBT_ENV_SECRET_AUTHENTICATOR"] = str(creds.get("authenticator") or "")
    os.environ[f"{env_prefix}_DBT_ENV_SECRET_ROLE"] = str(creds.get("role") or "")
    os.environ[f"{env_prefix}_DBT_ENV_SECRET_WAREHOUSE"] = str(creds.get("warehouse") or "")
    os.environ[f"{env_prefix}_DBT_ENV_SECRET_DATABASE"] = str(creds.get("database") or "")

    fixed_schema = os.getenv("FIXED_DBT_SCHEMA")
    if not fixed_schema:
        raise ValueError("FIXED_DBT_SCHEMA environment variable is not set.")

    os.environ[f"{env_prefix}_DBT_ENV_SECRET_SCHEMA"] = fixed_schema

    logger.info(f"Environment variables set with fixed schema '{fixed_schema}' for target '{target_env}'.")
    
    # # Use the model name as schema
    # os.environ[f"{env_prefix}_DBT_ENV_SECRET_SCHEMA"] = str(model or "")

    # logger.info(f" Environment variables set for model: {model} under target: {target_env}")

   

# @flow
# def test_set_env_vars():
#     os.environ["DBT_ENV_SECRET_TARGET"] = "dev"  #  Set target env
#     set_env_vars("my_schema_name")

#     # Optional: print a couple of env vars
#     print(" Env Var - ACCOUNT:", os.getenv("DEV_DBT_ENV_SECRET_ACCOUNT"))
#     print(" Env Var - SCHEMA:", os.getenv("DEV_DBT_ENV_SECRET_SCHEMA"))

# if __name__ == "__main__":
#     test_set_env_vars()



from prefect import task

@task
def print_working_directory() -> str:
    """
    Task to log and return the current working directory.
    """
    logger = get_run_logger()
    current_directory = os.getcwd()
    logger.info(f"Current working directory: {current_directory}")
    return current_directory

# @flow
# def debug_flow():
#     print_working_directory()

# if __name__ == "__main__":
#     debug_flow()

@task
def run_dbt_deps() -> str:
    """
    Runs 'dbt deps' to install dependencies listed in packages.yml.

    Returns:
        str: The result output from the 'dbt deps' command.
    """
    logger = get_run_logger()

    try:
        result = DbtCoreOperation(
            commands=["dbt deps"],
            project_dir=project_dir,
            profiles_dir=profiles_dir
        ).run()

        logger.info(" dbt deps completed successfully.")
        return result

    except Exception as e:
        logger.error(f" dbt deps failed: {e}")
        raise


# if __name__ == "__main__":
#     run_dbt_deps()
@task
def run_dbt_debug() -> str:
    """
    Runs the 'dbt debug' command and returns the result.

    Returns:
        str: The result of the 'dbt debug' command.
    """
    result = DbtCoreOperation(
        commands=["dbt debug"],
        project_dir=project_dir,
        profiles_dir=profiles_dir
    ).run()
    logger = get_run_logger()
    logger.info("dbt debug completed.")
    return result
# if __name__ == "__main__":
#     run_dbt_debug()

@task
def run_dbt_seed(model: str, full_refresh: bool) -> str:
    """
    Runs the dbt seed command to load data into the staging tables.

    Args:
        model (str): The name of the seed file or tag to run.
        full_refresh (bool): If True, runs seed with --full-refresh.

    Returns:
        str: Output from the dbt seed command.
    """
    logger = get_run_logger()

    # Build dbt seed command
    dbt_seed_command = ["dbt seed", f"--select {model}"]
    
    if full_refresh:
        dbt_seed_command.append("--full-refresh")
        logger.info("Running dbt seed with --full-refresh")

    logger.info(f"Running dbt seed for model: {model}")

    result = DbtCoreOperation(
        commands=[" ".join(dbt_seed_command)],
        project_dir=project_dir,
        profiles_dir=profiles_dir
    ).run()

    logger.info("dbt seed completed successfully.")
    return result


# @flow
# def test_seed():
#     output = run_dbt_seed(model="your_seed_model", full_refresh=True)
#     print("DBT Seed Output:", output)

# if __name__ == "__main__":
#     test_seed()



@task
def run_dbt_run(model: str, full_refresh: bool) -> str:
    logger = get_run_logger()
    dbt_run_command = ["dbt run", f"--select {model}"]

    if full_refresh:
        dbt_run_command.append("--full-refresh")
        logger.info("Running dbt run with --full-refresh.")

    logger.info(f"Executing dbt run for model: {model}")

    result = DbtCoreOperation(
        commands=[" ".join(dbt_run_command)],
        project_dir=project_dir,
        profiles_dir=profiles_dir
    ).run()

    logger.info("dbt run completed.")
    return result


# @flow
# def test_dbt_run():
#     output = run_dbt_run(model="your_model_name", full_refresh=True)
#     print("DBT Run Output:", output)


# if __name__ == "__main__":
#     test_dbt_run()


@task
def run_dbt_schema(model: str, full_refresh: bool, seeds_excluded: bool):
    set_env_vars(model)
    target_env = os.getenv("DBT_ENV_SECRET_TARGET")
    env_prefix = {"dev": "DEV", "stage": "STAGE", "production": "PROD"}.get(target_env)
    
    schema = os.getenv(f"{env_prefix}_DBT_ENV_SECRET_SCHEMA")
    print(f"Running dbt commands with schema: {schema}")
    
    dbt_debug_result = run_dbt_debug()
    dbt_seed_result = None
    if not seeds_excluded:
        dbt_seed_result = run_dbt_seed(model, full_refresh)
    dbt_run_result = run_dbt_run(model, full_refresh)
    return dbt_seed_result, dbt_run_result, dbt_debug_result



@flow
def run_all_dbt_tasks():
    result = run_dbt_schema(
        model="my_model",
        full_refresh=True,
        seeds_excluded=False
    )
    print("Final result:", result)
if __name__ == "__main__":
    run_all_dbt_tasks()

@flow
def trigger_dbt_flow(full_refresh: bool = False, models=None,
                     seed_exclusions=None, model_exclusions=None):
    """
    A Prefect flow to run dbt commands.

    This flow runs the following dbt commands:
    1. Print the current working directory.
    2. Run `dbt debug` to check the dbt project configuration.
    3. Run `dbt seed` to load CSV data files.
    4. Run `dbt run` to execute dbt models.

    Args:
        full_refresh (bool): Whether to run dbt commands with the --full-refresh flag.
        models (list[str], optional): A list of model names to run with full-refresh.
        seed_exclusions (list[str], optional): A list of models or subdirectories to exclude.
        model_exclusions (list[str], optional): A list of models or subdirectories to exclude.

    Returns:
        tuple: Results of running dbt commands.
    """
    logger = get_run_logger()
    logger.info("Starting dbt flow...")

    # Run the tasks in sequence
    current_directory = print_working_directory()
    dbt_deps_result = run_dbt_deps()
    # dbt_debug_result = run_dbt_debug()

    dbt_models = get_dbt_models()
    if models:
        if not all(model in dbt_models for model in models):
            raise ValueError("Invalid model names provided.")
    if model_exclusions:
        if not all(model in dbt_models for model in model_exclusions):
            raise ValueError("Invalid model names provided.")
    if seed_exclusions:
        if not all(model in dbt_models for model in seed_exclusions):
            raise ValueError("Invalid model names provided.")

    models = models or dbt_models
    model_exclusions = model_exclusions or []
    seed_exclusions = seed_exclusions or []
    models = [model for model in models if model not in model_exclusions]

    dbt_seed_result = []
    dbt_run_result = []
    dbt_debug_result = []
    for model in models:
        seeds_excluded = any(model in seed_exclusions for model in models)
        seed_result, run_result, debug_result = run_dbt_schema(
            model, full_refresh, seeds_excluded
            )
        dbt_seed_result.append(seed_result)
        dbt_run_result.append(run_result)
        dbt_debug_result.append(debug_result)

    logger.info("dbt flow completed.")
    return current_directory, dbt_deps_result, dbt_debug_result, dbt_seed_result, dbt_run_result




if __name__ == "__main__":
    trigger_dbt_flow(
        full_refresh=False,
        models=None,
        # seed_exclusions=["haptiq"],
        # model_exclusions=["haptiq"]
    )
