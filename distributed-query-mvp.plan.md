<!-- 006b1184-40a4-4c16-a95d-034867145156 5604bc37-5836-40be-9bbe-20bfe837360b -->
# MVP de Distributed Query Processing (Dremel-like)

## Arquitetura Geral

O sistema seguirá uma arquitetura inspirada no Google Dremel com três camadas principais:

1. **Root Server (Coordinator)**: Recebe queries via API REST, cria query tree, distribui trabalho e agrega resultados
2. **Intermediate Servers**: Agregam resultados parciais de múltiplos workers (para escalabilidade)
3. **Leaf Servers (Workers)**: Executam queries sobre partições de dados colunares locais

## Estrutura do Projeto

```
/
├── cmd/
│   ├── coordinator/     # Servidor coordenador (root)
│   ├── worker/          # Servidor worker (leaf)
│   └── cli/             # CLI para carregar dados e testes
├── internal/
│   ├── parser/          # Parser SQL para AST
│   ├── planner/         # Query planner e otimizador
│   ├── executor/        # Executores de operações (scan, filter, join, aggregate)
│   ├── storage/         # Storage engine colunar
│   ├── distributed/     # Protocolo de comunicação entre nós
│   ├── api/             # Handlers da API REST
│   └── visualizer/      # Geração de visualização da query tree
├── pkg/
│   ├── columnar/        # Formato de dados colunar
│   └── query/           # Estruturas de query comuns
├── deployments/
│   ├── docker/          # Dockerfiles e docker-compose
│   └── k8s/             # Manifests Kubernetes (opcional)
├── data/                # Dados de exemplo
└── web/                 # Frontend simples para visualização (opcional)
```

## Componentes Principais

### 1. Storage Engine Colunar (`internal/storage`)

- Implementar formato de armazenamento colunar em disco
- Suporte a tipos básicos: INT, STRING, FLOAT, BOOL
- Particionamento de dados por ranges ou hash
- Serialização eficiente (pode usar encoding/gob ou Protocol Buffers)
- Metadata de schemas e estatísticas por coluna

### 2. Parser SQL (`internal/parser`)

- Parser para queries SQL básicas: SELECT, WHERE, JOIN, GROUP BY, ORDER BY
- Converter SQL string em AST (Abstract Syntax Tree)
- Validação básica de sintaxe
- Pode usar biblioteca existente como `xwb1989/sqlparser` ou implementar parser simples manualmente

### 3. Query Planner (`internal/planner`)

- Receber AST e criar execution plan
- Construir query tree distribuída:
  - Nós folha: Scan e Filter
  - Nós intermediários: Join, Aggregate, Sort
  - Nó raiz: Final aggregation
- Otimizações básicas: push-down de filtros, reordenação de joins
- Determinar particionamento e distribuição de trabalho

### 4. Query Executor (`internal/executor`)

- **ScanExecutor**: Lê dados colunares do storage
- **FilterExecutor**: Aplica predicados WHERE
- **JoinExecutor**: Join hash-based ou nested loop
- **AggregateExecutor**: GROUP BY e funções (SUM, COUNT, AVG, MIN, MAX)
- **SortExecutor**: ORDER BY
- Cada executor processa dados em batches (row batches estilo Arrow)

### 5. Distributed Coordination (`internal/distributed`)

- Protocolo de comunicação entre coordinator e workers (gRPC ou HTTP/JSON)
- Coordinator distribui sub-queries para workers
- Workers executam queries locais e retornam resultados parciais
- Coordinator agrega resultados finais
- Implementar MapReduce pattern: Map (workers) → Shuffle → Reduce (coordinator)

### 6. API REST (`internal/api`)

Endpoints principais:

- `POST /query` - Submeter query SQL
  - Body: `{"sql": "SELECT ... FROM ... WHERE ..."}`
  - Response: Resultados + query tree metadata
- `GET /query/{id}` - Status de query assíncrona
- `GET /query/{id}/tree` - Visualização da query tree (JSON ou DOT format)
- `POST /data/load` - Carregar dados de exemplo
- `GET /health` - Health check dos workers

### 7. Query Tree Visualizer (`internal/visualizer`)

- Gerar representação visual da query tree
- Formatos de saída:
  - **JSON**: Para renderização em frontend (D3.js, vis.js)
  - **DOT**: Para Graphviz (gerar imagens PNG/SVG)
- Incluir informações: tipo de nó, predicados, estatísticas de execução (rows processadas, tempo)

### 8. Coordinator Server (`cmd/coordinator`)

- Servidor HTTP/REST para receber queries
- Mantém registro de workers disponíveis (service discovery simples)
- Distribui trabalho usando query planner
- Agrega resultados finais
- Suporte a execução assíncrona com query ID

### 9. Worker Server (`cmd/worker`)

- Servidor que registra com coordinator
- Armazena partições de dados localmente
- Executa sub-queries recebidas do coordinator
- Retorna resultados parciais

### 10. Deployment

**Simulação Local**:

- Iniciar múltiplos workers em portas diferentes
- Coordinator conecta com workers via localhost:port

**Docker Compose**:

- Container para coordinator
- N containers para workers (scale workers=5)
- Volume compartilhado ou distribuído para dados
- Rede Docker para comunicação

**Kubernetes** (opcional):

- Deployment do coordinator (1 replica)
- Deployment de workers (N replicas com HPA)
- Service para descoberta de workers
- ConfigMap para configurações

## Exemplo de Fluxo

1. Cliente envia query: `SELECT user_id, COUNT(*) FROM events WHERE date > '2025-01-01' GROUP BY user_id`
2. Coordinator parseia SQL → AST
3. Planner cria query tree:
   ```
   Root: Final Aggregate
     ├─ Worker1: Scan(partition1) → Filter → Local Aggregate
     ├─ Worker2: Scan(partition2) → Filter → Local Aggregate
     └─ Worker3: Scan(partition3) → Filter → Local Aggregate
   ```

4. Coordinator distribui sub-queries para workers
5. Workers processam em paralelo e retornam resultados parciais
6. Coordinator agrega resultados finais
7. API retorna JSON com resultados + visualização da tree

## Dados de Exemplo

Criar datasets de exemplo:

- **Tabela `events`**: user_id, event_type, timestamp, value
- **Tabela `users`**: user_id, name, country
- Gerar ~1M registros para demonstrar escalabilidade
- Particionar dados em 3-5 partições

## Demonstração de Escalabilidade

- Benchmark: Medir tempo de query com 1, 2, 4, 8 workers
- Mostrar speedup linear ou sub-linear
- Gráficos de performance (tempo vs número de workers)
- Comparar execução local vs distribuída

## Tecnologias e Bibliotecas Go

- **Web framework**: `gin-gonic/gin` ou `gorilla/mux`
- **gRPC** (opcional): Comunicação mais eficiente entre nós
- **SQL Parser**: `xwb1989/sqlparser` ou custom
- **Serialização**: Protocol Buffers ou encoding/gob
- **Visualização**: Gerar DOT format para Graphviz
- **Testing**: Testes unitários e de integração

## Documentação

- README principal com overview da arquitetura
- README de deployment (como rodar localmente e com Docker)
- Documentação da API (endpoints e exemplos)
- Diagramas de arquitetura (pode usar Mermaid ou Draw.io)

### To-dos

- [ ] Criar estrutura de diretórios do projeto e módulo Go com dependências
- [ ] Implementar storage engine colunar com serialização e particionamento
- [ ] Implementar parser SQL básico para converter queries em AST
- [ ] Implementar executores básicos (Scan, Filter, Aggregate, Join)
- [ ] Implementar query planner para criar query tree distribuída
- [ ] Implementar protocolo de comunicação entre coordinator e workers
- [ ] Implementar servidor worker que executa sub-queries
- [ ] Implementar servidor coordinator que distribui queries e agrega resultados
- [ ] Implementar API REST para submissão de queries
- [ ] Implementar visualizador de query tree (JSON/DOT format)
- [ ] Criar script para gerar dados de exemplo e carregar no sistema
- [ ] Criar Dockerfiles e docker-compose para deployment distribuído
- [ ] Criar testes de integração e benchmarks de escalabilidade
- [ ] Criar documentação (README, API docs, diagramas de arquitetura)