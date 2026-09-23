# Guia de Configuração - Streaming Kafka com Databricks

## Pré-requisitos

1. **Databricks Workspace** - Você precisa de acesso a um workspace Databricks
2. **Cluster Kafka** - Cluster Confluent Cloud ou Kafka self-hosted
3. **Storage Externo** - S3 (AWS) ou ADLS (Azure) para checkpoints e tabelas Delta
4. **Cluster Databricks** - Com configurações Spark apropriadas

## Configuração do Kafka

### Setup do Confluent Cloud

1. Acesse [Confluent Cloud](https://confluent.cloud/)
2. Crie um cluster ou use um existente
3. Crie API Key e Secret
4. Crie tópicos (ex: `sample_data_users`)
5. Anote:
   - Bootstrap servers (ex: `pkc-xxxxx.eastus.azure.confluent.cloud:9092`)
   - API Key
   - API Secret
   - Nomes dos tópicos

### Atualizar Parâmetros dos Notebooks

Substitua os valores placeholder em cada notebook:

```python
confluentBootstrapServers = "seus-bootstrap-servers"
confluentTopicName = "seu-nome-topico"
confluentApiKey = "sua-api-key"
confluentSecret = "seu-api-secret"
```

## Configuração de Storage Externo

### AWS S3

1. Crie um bucket S3 para checkpoints e tabelas Delta
2. Configure role IAM com permissões apropriadas
3. Atualize o notebook com:
   ```python
   checkpoint_location = "s3://seu-bucket/checkpoints/seu-checkpoint"
   ```

### Azure Data Lake Storage (ADLS)

1. Crie uma conta de storage e container
2. Configure credenciais de acesso
3. Atualize o notebook com:
   ```python
   checkpoint_location = "abfss://container@storageaccount.dfs.core.windows.net/checkpoints/seu-checkpoint"
   ```

## Configuração do Cluster Databricks

### Bibliotecas Necessárias

Instale estas bibliotecas no seu cluster:

```json
{
  "libraries": [
    {
      "maven": {
        "coordinates": "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "exclusions": ["org.slf4j:slf4j-log4j12"]
      }
    }
  ]
}
```

### Configurações do Cluster

- **Versão Spark**: 3.5.x ou superior
- **Versão Scala**: 2.12
- **Versão Python**: 3.10 ou superior
- **Tipo de Worker**: Baseado no volume de dados
- **Tipo de Driver**: Baseado nos seus requisitos

## Uso dos Notebooks

### 1. Streaming Básico (read_stream_from_kafka.py)

Use este notebook para:
- Testar sua conexão Kafka
- Verificar formato dos dados
- Entender estrutura do schema

### 2. Streaming Contínuo para Bronze (kafka_to_bronze_streaming.py)

Use este notebook para:
- Criar um pipeline de streaming contínuo
- Ingerir dados para tabela Bronze
- Monitorar fluxo de dados em tempo real

### 3. Delta Live Tables (kafka_dlt_bronze.py)

Use este notebook para:
- Criar pipelines de produção automatizados
- Implementar regras de qualidade de dados
- Gerenciar evolução de schema
- Simplificar operações

### 4. Múltiplos Tópicos (multiple_kafka_topics_bronze.py)

Use este notebook para:
- Processar múltiplos tópicos Kafka simultaneamente
- Criar tabelas Bronze separadas por tópico
- Fazer join de dados de diferentes tópicos

### 5. Transformações em Tempo Real (kafka_realtime_transformations.py)

Use este notebook para:
- Aplicar transformações em tempo real
- Adicionar lógica de negócio durante ingestão
- Criar métricas em tempo real
- Monitorar qualidade de dados

## Melhores Práticas

### Gerenciamento de Checkpoints

- Use localizações de checkpoint separadas para cada query de streaming
- Armazene checkpoints na mesma região do seu workspace Databricks
- Não delete checkpoints a menos que queira reiniciar do início

### Gerenciamento de Schema

- Defina schemas explicitamente para melhor performance
- Use opção `mergeSchema` para evolução de schema
- Monitore mudanças de schema em produção

### Otimização de Performance

- Use particionamento para tabelas grandes
- Habilite auto-compaction e optimize write
- Configure intervalos de trigger apropriados baseados em requisitos de latência
- Use watermarking para operações com estado

### Monitoramento

- Monitore status de queries de streaming regularmente
- Verifique métricas de latência de processamento
- Configure alertas para falhas
- Revise progresso e estatísticas das queries

## Solução de Problemas

### Problemas de Conexão

```python
# Testar conexão Kafka
df_test = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", confluentBootstrapServers) \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", f'...') \
    .option("subscribe", confluentTopicName) \
    .load()
```

### Incompatibilidade de Schema

- Verifique se seu schema corresponde aos dados do tópico Kafka
- Verifique estruturas JSON aninhadas
- Use `display(df_raw)` para inspecionar dados brutos

### Problemas de Performance

- Aumente recursos do cluster
- Ajuste intervalos de trigger
- Revise estratégia de particionamento
- Verifique skew na distribuição de dados

### Problemas de Checkpoint

- Certifique-se que a localização do checkpoint está acessível
- Verifique espaço de armazenamento suficiente
- Verifique IAM/permissões
- Delete e recrie checkpoint se estiver corrompido

## Considerações de Segurança

- Nunca commit API keys para controle de versão
- Use Databricks Secrets para informações sensíveis
- Habilite SSL/TLS para conexões Kafka
- Implemente roles IAM apropriadas para acesso ao storage
- Use endpoints VPC para conectividade privada

## Próximos Passos

1. Comece com o exemplo básico para verificar conectividade
2. Progrida para streaming contínuo para uso em produção
3. Implemente Delta Live Tables para pipelines automatizados
4. Adicione regras de qualidade de dados e monitoramento
5. Escale para múltiplos tópicos e transformações complexas

## Suporte

Para problemas ou dúvidas:
- Consulte documentação Databricks: https://docs.databricks.com/
- Documentação Kafka: https://kafka.apache.org/documentation/
- Documentação Confluent Cloud: https://docs.confluent.io/