import os
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger
from prefect.blocks.notifications import EmailServerCredentials, EmailMessage

# Load environment variables from .env
load_dotenv()

@task
def dummy_task():
    logger = get_run_logger()
    logger.info("Doing something useful...")
    return "success"

@task
def send_email_notification(subject: str, body: str):
    # Using your block name: "emailservercredintials"
    email_credentials = EmailServerCredentials.load("emailservercredintials")

    # Read recipient from environment
    email_to = os.getenv("ALERT_EMAIL_TO")
    if not email_to:
        raise ValueError("⚠️ ALERT_EMAIL_TO is not set in .env")

    # Create and send the email
    email_message = EmailMessage(
        subject=subject,
        body=body,
        email_to=[email_to],
        email_from=email_credentials.username
    )
    email_message.send(email_credentials=email_credentials)

@flow
def email_notification_flow():
    try:
        result = dummy_task()
        send_email_notification(
            subject="✅ Prefect Flow Succeeded",
            body=f"The task succeeded with result: {result}"
        )
    except Exception as e:
        send_email_notification(
            subject="❌ Prefect Flow Failed",
            body=f"The task failed with error: {str(e)}"
        )
        raise

if __name__ == "__main__":
    email_notification_flow()
