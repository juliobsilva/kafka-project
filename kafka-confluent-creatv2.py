import json
import os
import sys
from confluent_kafka.admin import AdminClient, ConfigResource, ConfigEntry, AlterConfigOpType, NewTopic
from confluent_kafka.error import KafkaException


def topic_name_normalized(domain, environment, date_type, date_name):
    # Normaliza os valores de entrada
    domain_normalized = domain.lower()
    environment_normalized = environment.lower()
    date_type_normalized = date_type.lower()
    date_name_normalized = date_name.lower()

    # Gera o nome do tópico seguindo o template
    normalized_kafka_topic_name = f'{domain_normalized}-{environment_normalized}-{date_type_normalized}-{date_name_normalized}'
    
    return normalized_kafka_topic_name

def create_kafka_topic(admin_client, normalized_kafka_topic_name, environment, num_partitions, replication_factor):

    environment = environment
    num_partitions = num_partitions
    replication_factor = replication_factor

    # Definição do novo tópico
    if environment == "PR":
        new_topic = NewTopic(topic=normalized_kafka_topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
    else:
        new_topic = NewTopic(topic=normalized_kafka_topic_name, num_partitions=2, replication_factor=3)

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

    # Variáveis de ambiente  
    domain = str(os.getenv('DOMAIN'))
    environment = str(os.getenv('ENVIRONMENT'))
    date_type = str(os.getenv('DATE_TYPE'))
    date_name = str(os.getenv('DATE_NAME'))
    retention_ms = int(os.getenv('RETENTION_MS')) if os.getenv('RETENTION_MS') else None
    max_message_bytes = int(os.getenv('MAX_MESSAGE_BYTES'))if os.getenv('MAX_MESSAGE_BYTES') else
    num_partitions = int(os.getenv('NUM_PARTITIONS', 1))if os.getenv('NUM_PARTITIONS') else None
    replication_factor = int(os.getenv('REPLICATION_FACTOR', 1))if os.getenv('REPLICATION_FACTOR') else None

    config_dicts = {
        "retention.ms": "7200000",  
        "max.message.bytes": "1048576"
    }
    
    # Configurações específicas para o ambiente de produção
    if environment == "PR":
        config_dicts["retention.ms"] = retention_ms
        config_dicts["max.message.bytes"] = max_message_bytes
        num_partitions = num_partitions
        replication_factor = replication_factor

    # Configuração do cliente Kafka
    kafka_credentials = json.loads(os.getenv('KAFKA_CREDENTIALS'))
    admin_client = AdminClient(kafka_credentials)

    # Verifica se os parâmetros foram informados
    if all(var not in (None, '') for var in [domain, environment, date_type, date_name]):
        normalized_kafka_topic_name = topic_name_normalized(domain, environment, date_type, date_name)
        create_result  = create_kafka_topic(admin_client, normalized_kafka_topic_name, environment, num_partitions, replication_factor)
        if create_result == 0:    
            set_default_config(admin_client, normalized_kafka_topic_name, config_dicts)
            sys.exit(create_result ) 
    else:
        if any(param in (None, '') for param in [domain, environment, date_type, date_name]):
            # Identifica qual parâmetro está faltando
            for param, name in zip([domain, environment, date_type, date_name], ['domain', 'environment', 'date_type', 'date_name']):
                if param in (None, ''):
                    print(f"O parâmetro {name} não foi informado")
        else:
            print("Não há tópico a ser criado")  

if __name__ == "__main__":
    main()