from prefect import flow, task
import subprocess
import os

DBT_PROJECT_DIR = "./my_dbt_tasks"  

@task
def run_dbt_model(model_name: str):
    result = subprocess.run(
        ["dbt", "run", "--select", model_name],
        capture_output=True,
        text=True,
        cwd=DBT_PROJECT_DIR  
    )
    print(result.stdout)
    if result.returncode != 0:
        print("Error:", result.stderr)
        raise Exception(f"DBT run failed for model: {model_name}")

@flow(name=" DBT Model Flow")
def dbt_flow():
    run_dbt_model("products")
    run_dbt_model("sample_model")

if __name__ == "__main__":
    dbt_flow()
