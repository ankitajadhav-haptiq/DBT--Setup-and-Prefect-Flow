# from prefect import flow, task
# from prefect_shell import ShellOperation

# @task
# def run_dbt_sample_model():
#     result = ShellOperation(
#         commands=["dbt run --select sample_model"],
#         working_dir="./my_dbt_tasks"
#     ).run()
#     print(result)
#     return result

# @flow
# def dbt_sample_model_flow():
#     run_dbt_sample_model()

# if __name__ == "__main__":
#     dbt_sample_model_flow()


from prefect import flow, task
import subprocess

DBT_PROJECT_DIR= "./my_dbt_tasks"

@task
def run_prefect_dbt_sample_model(model_name:str):
    result=subprocess.run(
        ["dbt","run","--select",model_name],
        capture_output=True,
        text=True,
        cwd=DBT_PROJECT_DIR
    )
    print(result.stdout)
    if result.returncode != 0:
        print("error:",result.stderr)
        raise Exception(f"DBT run failed for model: {model_name}")
    
@flow(name="DBT sample_model_flow")
def dbt_sample_model_flow():
    run_prefect_dbt_sample_model("sample_model")

if __name__ == "__main__":
    dbt_sample_model_flow()