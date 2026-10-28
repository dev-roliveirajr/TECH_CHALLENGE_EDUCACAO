# =====================================================================
# Makefile para automação de comandos do projeto
# Copie este arquivo para o repositório raiz renomeando para 'Makefile'
# =====================================================================

.PHONY: help install format lint test aws clean

help:
	@echo "Comandos disponíveis:"
	@echo "  make install   - Cria .venv e instala dependências"
	@echo "  make format    - Formata código com black"
	@echo "  make lint      - Valida conformidade PEP 8 com flake8"
	@echo "  make test      - Executa suite de testes com pytest"
	@echo "  make aws      	- Executa o setup e deploy dos scripts e jobs na AWS"
	@echo "  make clean     - Remove caches e arquivos temporários"

install:
	python3 -m venv .venv
	. .venv/bin/activate && pip install --upgrade pip
	. .venv/bin/activate && pip install -r requirements.txt
	. .venv/bin/activate && pre-commit install

format:
	black src/ tests/

lint:
	flake8 src/ tests/

test:
	pytest --cov=src tests/

aws:
	python src/infra/deploy_aws_scripts.py

clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	rm -rf .pytest_cache .coverage htmlcov .venv
