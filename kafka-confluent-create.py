#!/usr/bin/env python3

import logging
import json
import os
import sys
from confluent_kafka.admin import AdminClient, ConfigResource, ConfigEntry, AlterConfigOpType, NewTopic
from confluent_kafka.error import KafkaException, KafkaError


def topic_name_normalized(cost_center, domain, environment, data_type, data_name):
    # Normaliza os valores de entrada
    domain_normalized = domain.lower()
    environment_normalized = environment.lower()
    data_type_normalized = data_type.lower()
    data_name_normalized = data_name.lower()
    cost_center_normalized = cost_center.lower()

    # Gera o nome do tópico seguindo o template
    normalized_kafka_topic_name = f'{cost_center_normalized}-{domain_normalized}-{environment_normalized}-{data_type_normalized}-{data_name_normalized}'
    
    return normalized_kafka_topic_name

def create_kafka_topic(admin_client, normalized_kafka_topic_name, environment, num_partitions, replication_factor):

    environment = environment
    num_partitions = num_partitions
    replication_factor = replication_factor

    # Definição do novo tópico
    if environment == "PR":
        new_topic = NewTopic(topic=normalized_kafka_topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
    else:
        new_topic = NewTopic(topic=normalized_kafka_topic_name, num_partitions=num_partitions, replication_factor=replication_factor)

    try:
        # Verifica se o tópico já existe
        metadata = admin_client.list_topics(timeout=10)
        if normalized_kafka_topic_name in metadata.topics:
            print(f"O tópico '{normalized_kafka_topic_name}' já existe.")
            return 1

        # Tentativa de criar o tópico
        result = admin_client.create_topics([new_topic])

        # Verificação do resultado
        for topic, future in result.items():
            try:
                future.result() 
                print(f"Tópico '{topic}' criado com sucesso.")
            except KafkaException as e:
                print(f"Falha ao criar o tópico '{topic}': {e}")
                return 1
        return 0
    except KafkaException as e:
        print(f"Erro ao tentar verificar ou criar o tópico: {e}")
        return 1
    
def set_default_config(admin_client, topic_name, config_dicts):

    try:
        # Criação de objetos ConfigEntry para cada configuração a ser definida
        config_entries = [ConfigEntry(name=name, value=str(value), incremental_operation=AlterConfigOpType['SET'])
                        for name, value in config_dicts.items()]
        
        # Criação de ConfigResource com a lista de configurações
        resource = ConfigResource('topic', topic_name, incremental_configs=config_entries)        
        result_dict = admin_client.incremental_alter_configs([resource])
        result_dict[resource].result()
    except KafkaException as e:
        print(f"Erro ao tentar definir as configurações: {e}")
        raise    

def main():
    try:
        # Obtém variáveis de ambiente
        domain = os.getenv('DOMAIN', '').strip()
        environment = os.getenv('ENVIRONMENT', '').strip()
        data_type = os.getenv('DATA_TYPE', '').strip()
        data_name = os.getenv('DATA_NAME', '').strip()
        cost_center = os.getenv('COST_CENTER', '').strip()
    
        # Validação dos valores de entrada
        try:
            retention_ms = int(os.getenv('RETENTION_MS'))
            max_message_bytes = int(os.getenv('MAX_MESSAGE_BYTES'))
            num_partitions = int(os.getenv('NUM_PARTITIONS'))
            replication_factor = int(os.getenv('REPLICATION_FACTOR'))
        except ValueError as e:
            logging.error(f"Erro ao tentar converter os valores de entrada: {e}")
            sys.exit(1)

        # Configurações padrão
        config_dicts = {
            "retention.ms": "7200000",  
            "max.message.bytes": "1048576"
        }

        # Configurações específicas para o ambiente de produção
        if environment == "PR":
            config_dicts["retention.ms"] = retention_ms
            config_dicts["max.message.bytes"] = max_message_bytes
            if not (2 <= replication_factor <= 3):
                logging.error(f"Fator de replicação inválido: {replication_factor}. Deve ser no mínimo (2) ou no máximo (3).")
                sys.exit(1)
        else:
            # Valida número de partições para ambientes não produtivos
            if not (1 <= num_partitions <= 3):
                logging.error(f"Número de partições inválido: {num_partitions}. Deve ser no mínimo (1) ou no máximo (3).")
                sys.exit(1)
            
            # Valida fator de replicação para ambientes não produtivos
            if not (2 <= replication_factor <= 3):
                logging.error(f"Fator de replicação inválido: {replication_factor}. Deve ser no mínimo (2) ou no máximo (3).")
                sys.exit(1)
        
        # Configuração do cliente Kafka
        try:
            kafka_credentials = json.loads(os.getenv('KAFKA_CREDENTIALS'))
            admin_client = AdminClient(kafka_credentials)
        except (json.JSONDecodeError, KeyError) as e:
            logging.error(f"Erro ao configurar o cliente Kafka: {e}")
            sys.exit(1)
        
        # Verifica se todos os parâmetros obrigatórios foram informados
        if not all([cost_center, domain, environment, data_type, data_name]):
            missing_params = [name for param, name in zip(
                [cost_center, domain, environment, data_type, data_name], 
                ['COST_CENTER', 'DOMAIN', 'ENVIRONMENT', 'DATA_TYPE', 'DATA_NAME']
            ) if not param]
            logging.error(f"Os seguintes parâmetros não foram informados: {', '.join(missing_params)}")
            sys.exit(1)
        else:
            normalized_kafka_topic_name = topic_name_normalized(
                cost_center, domain, environment, data_type, data_name
            )
            try:
                create_result = create_kafka_topic(
                    admin_client, normalized_kafka_topic_name, environment, num_partitions, replication_factor
                )
                if create_result == 0:
                    set_default_config(admin_client, normalized_kafka_topic_name, config_dicts)
                    sys.exit(create_result)
            except Exception as e:
                logging.error(f"Erro ao criar o tópico Kafka: {e}")
                sys.exit(1)
    
    except (Exception, KafkaError) as e:
        logging.error(f"Erro inesperado no main: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()