.PHONY: build up config status down destroy

build:
	docker compose build

up:
	docker compose up -d

config:
	docker compose config --quiet

status:
	docker compose ps -a

down:
	docker compose down

destroy:
	docker compose down --volumes --remove-orphans
