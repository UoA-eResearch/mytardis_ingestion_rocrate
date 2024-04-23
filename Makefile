poetry_setup:
	poetry config virtualenvs.in-project true
	poetry lock
	poetry install

poetry_activate:
	poetry update
	poetry shell

