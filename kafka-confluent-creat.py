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

        default_configurations = {
            'retention.ms': '7200000',  
            'max.message.bytes': '1048576'
        }

        config_resource = ConfigResource(ConfigResource.Type.TOPIC, normalized_kafka_topic_name, configs=default_configurations)
        admin_client.incremental_alter_configs([config_resource])

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

    # Configuração do cliente Kafka
    admin_client = AdminClient({'bootstrap.servers': '13.92.98.80:9092'})

    normalized_kafka_topic_name = topic_name_normalized(domain, environment, date_type, date_name)
    exit_code = create_kafka_topic(admin_client, normalized_kafka_topic_name)
    sys.exit(exit_code)

if __name__ == "__main__":
    main()