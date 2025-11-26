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

## Execução via Docker Compose

1. **Gerar dados no host (opcional, mas recomendado para testar mais fácil)**  
   No diretório `src/`:
   - **Instale dependências**: `go mod download`  
   - **Gere dados sintéticos** (por exemplo, 5000 linhas):  
     `go run ./cmd/cli --rows 5000`  
   Isso vai criar/atualizar o diretório `src/data`, que contém as partições colunares (`*.gob`).

2. **Subir coordinator + workers com Docker Compose**  
   No diretório `src/deployments/docker/`, execute:
   - `docker compose up --build`  
   (ou `docker-compose up --build`, dependendo da sua instalação)

   O arquivo `docker-compose.yml`:
   - monta o diretório `src/data` do host como `/data` dentro de todos os containers  
   - inicia 1 coordinator escutando em `:8080`  
   - inicia 2 workers externos apontando para o coordinator

3. **Usar a API a partir do host**  
   Com os containers no ar:
   - O coordinator estará disponível em `http://localhost:8080`  
   - Você pode:
     - carregar mais dados via `POST /data/load`
     - submeter queries em `POST /query`
     - recuperar resultados em `GET /query/{id}` (campo `rows`)
     - inspecionar o contrato em `/swagger`
