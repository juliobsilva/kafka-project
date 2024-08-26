import argparse
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

def create_kafka_topic(admin_client, normalized_kafka_topic_name):

    # Definição do novo tópico
    new_topic = NewTopic(topic=normalized_kafka_topic_name, num_partitions=2, replication_factor=1)

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
        result_dict[resource].result()  # Wait for the result to ensure the configuration is applied
    except KafkaException as e:
        print(f"Erro ao tentar definir as configurações: {e}")
        raise

def main():
    # Configura o parser de argumentos
    parser = argparse.ArgumentParser(description='Cria e normaliza um nome de tópico do Kafka.')
    parser.add_argument('domain', type=str, help='Dominio')
    parser.add_argument('environment', type=str, help='Ambiente')
    parser.add_argument('date_type', type=str, help='Tipo do dado')
    parser.add_argument('date_name', type=str, help='Nome do dado')

    args = parser.parse_args()    

    # Recebe os parâmetros da linha de comando  
    domain = args.domain
    environment = args.environment
    date_type = args.date_type
    date_name = args.date_name

    config_dicts = {
        'retention.ms': 10,
        'cleanup.policy': 'compact'
    }
    # Configuração do cliente Kafka
    admin_client = AdminClient({'bootstrap.servers': '13.92.98.80:9092'})   

    normalized_kafka_topic_name = topic_name_normalized(domain, environment, date_type, date_name)
    create_kafka_topic = create_kafka_topic(admin_client, normalized_kafka_topic_name)
    set_default_config(admin_client, normalized_kafka_topic_name, config_dicts)
    sys.exit(create_kafka_topic)
    

if __name__ == "__main__":
    main()