.PHONY: migrate test lint format

migrate:
	alembic upgrade head

migration:
	alembic revision --autogenerate -m "$(m)"

test:
	pytest tests/ -v

lint:
	flake8 src/ --config=flake8.conf

format:
	black src/
	isort src/	