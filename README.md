# Streaming Databricks com Kafka

Este repositório contém exemplos práticos de pipelines de streaming de dados usando Kafka e Databricks:

## Notebooks

1. **read_stream_from_kafka.py** - Exemplo básico de leitura de tópico Kafka e exibição de dados
2. **kafka_to_bronze_streaming.py** - Exemplo completo de streaming contínuo do Kafka para tabela Bronze
3. **kafka_dlt_bronze.py** - Implementação de Delta Live Tables para pipeline automatizado
4. **multiple_kafka_topics_bronze.py** - Leitura de múltiplos tópicos Kafka simultaneamente
5. **kafka_realtime_transformations.py** - Transformações e enriquecimentos de dados em tempo real

## Recursos Principais

- **Streaming Contínuo**: Processa eventos em tempo real conforme chegam
- **Tabelas Bronze**: Ingestão de dados brutos com rastreamento de metadados
- **Delta Live Tables**: Gerenciamento automatizado de pipeline com regras de qualidade de dados
- **Suporte a Múltiplos Tópicos**: Processa múltiplos tópicos Kafka em paralelo
- **Transformações em Tempo Real**: Limpa, valida e enriquece dados dinamicamente
- **Watermarking**: Gerencia dados que chegam tarde com operações em janela

## Início Rápido

1. Configure os parâmetros de conexão Kafka em cada notebook
2. Configure sua localização externa para checkpoints (S3/ADLS)
3. Execute os notebooks no Databricks
4. Monitore as queries de streaming e consulte tabelas Bronze em tempo real

## Arquitetura

```
Tópicos Kafka → Spark Streaming → Tabelas Bronze → Tabelas Silver/Gold
     ↓                    ↓                ↓
  Eventos          Transformações      Analytics
```

Para arquitetura de produção detalhada, consulte:
- **ARCHITECTURE.md** - Documentação completa de arquitetura de produção
- **architecture-drawio.xml** - Diagrama de arquitetura visual (importe no Draw.io)
- **production-config-examples.py** - Exemplos de configuração prontos para produção

## Requisitos

- Databricks Workspace
- Cluster Kafka (Confluent Cloud ou self-hosted)
- Storage externo (S3 ou ADLS Gen2)
- Cluster Databricks com configurações Spark apropriadas

## Guia de Configuração

Consulte o arquivo `SETUP_GUIDE.md` para instruções detalhadas de:
- Configuração do Kafka
- Setup de storage externo
- Configuração do cluster Databricks
- Solução de problemas

## Licença

Este projeto é fornecido para fins educacionais e de demonstração.