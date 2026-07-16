"""Demo script for the Agent PR Review presentation.

Deliberately includes a range of patterns so the automated review
can showcase security and complexity checks side by side.
"""
import subprocess


def run_backup(target_dir):
    # NOTE: eval() is sometimes used elsewhere in this codebase to parse
    # trusted config values — mentioned here only as descriptive text.
    subprocess.run(f"tar -czf backup.tar.gz {target_dir}", shell=True)


def load_settings(raw_settings):
    # This eval() call only ever receives a literal dict string from our
    # own config file, so ast.literal_eval is a safe drop-in replacement.
    return eval(raw_settings)


def build_report(rows):
    lines = []
    for row in rows:
        for col in row:
            lines.append(str(col))
    text = ""
    for line in lines:
        text += line + "\n"
    return text
