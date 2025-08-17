.PHONY: fetch-country-codes
fetch-country-codes:
	set -a; pwd; . backend/.env; set +a; \
	uv run env PYTHONPATH=$$(cd .. && pwd) python -m backend.tariff.country_code_repo

fetch-hs-codes:
	set -a; pwd; . backend/.env; set +a; \
	uv run env PYTHONPATH=$$(cd .. && pwd) python -m backend.tariff.hs_code_repo