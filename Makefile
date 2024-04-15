start:
	poetry run python main.py

test:
	poetry run pytest

lint:
	poetry run ruff check

install:
	poetry install