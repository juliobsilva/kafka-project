import logging
import json
import os
import argparse
import sys
from confluent_kafka.admin import AdminClient, ConfigResource, ConfigEntry, AlterConfigOpType
from confluent_kafka.error import KafkaException

def get_config(admin_client, topic_name, config_name):
    
    try:
        resource = ConfigResource('topic', topic_name)
        result_dict = admin_client.describe_configs([resource])
        config_entries = result_dict[resource].result()
        if config_name in config_entries:
            config_property = config_entries[config_name]
            return config_property.value
        else:
            raise ValueError(f"Configuração não foi aplicada. Tópico, {config_name} não encontrado")    
    except KafkaException as e:
        print(f"Erro ao tentar obter a configuração: {e}")
        raise
    
def set_config(admin_client, topic_name, config_dicts):

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
    parser = argparse.ArgumentParser(description='Atualiza a configuração de um tópico do Kafka.')
    parser.add_argument('topic_name', type=str, help='Nome do tópico')
    
    # Recebe os parâmetros da linha de comando
    args = parser.parse_args()
    topic_name = args.topic_name
    
    # Configuração do cliente Kafka
    try:
        kafka_credentials = json.loads(os.getenv('KAFKA_CREDENTIALS'))
        admin_client = AdminClient(kafka_credentials)
    except (json.JSONDecodeError, KeyError, KafkaException, Exception) as e:
        logging.error(f"Erro ao configurar o cliente Kafka: {e}")
        sys.exit(1)
    
    # Define configurações para serem aplicadas
    config_dicts = {
        'retention.ms': '1048576',
    }
    try:
        set_config(admin_client, topic_name, config_dicts)
        
        for config_name in config_dicts.keys():
            new_value = get_config(admin_client, topic_name, config_name)
            print(f'A nova configuração {config_name} para o tópico {topic_name} é {new_value}')
        
        sys.exit(0)
    except KafkaException as e:
        print(f"Erro do Kafka ocorrido: {e}")
        sys.exit(1)
    except ValueError as e:
        print(f"Erro de valor ocorrido: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Erro inesperado ocorrido: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()