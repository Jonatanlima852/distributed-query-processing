# Distributed Query Processing MVP
MVP inspirado no Google Dremel para executar queries SQL distribuídas sobre dados colunares,
com coordinator, workers e ferramentas de visualização da query tree.

## Configuração de ambiente
1. Instale Go 1.24+ e habilite módulos (`GO111MODULE=on`).
2. Clone o repositório e execute `go mod download`.
3. Gere alguns dados sintéticos (opcional): `go run ./cmd/cli --rows 5000`.
4. Inicie o coordinator: `go run ./cmd/coordinator --http-addr :8080 --data-dir ./data --embedded-workers 1`.
5. (Opcional) Suba workers externos: `go run ./cmd/worker --id worker-1 --data-dir ./data --coordinator http://localhost:8080`.
6. Carregue dados adicionais via `POST /data/load` e submeta queries em `POST /query`.
7. Recupere o resultado completo em `GET /query/{id}` (campo `rows`) e explore o contrato via `/swagger` ou `docs/implementation/README.md`.
