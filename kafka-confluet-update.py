import argparse
import sys
from confluent_kafka.admin import AdminClient, ConfigResource, ConfigEntry, AlterConfigOpType
from confluent_kafka.error import KafkaException, KafkaError

def get_config(admin_client, topic_name, config_name):
    
    try:
        resource = ConfigResource('topic', topic_name)
        result_dict = admin_client.describe_configs([resource])
        config_entries = result_dict[resource].result()
        if config_name in config_entries:
            config_property = config_entries[config_name]
            return config_property.value
        else:
            raise ValueError(f"Configuration {config_name} not found for topic {topic_name}")    
    except KafkaException as e:
        print(f"Error while trying to get the configuration: {e}")
        raise
    
def set_config(admin_client, topic_name, config_dicts):

    try:
        # Create ConfigEntry objects for each config to set
        config_entries = [ConfigEntry(name=name, value=str(value), incremental_operation=AlterConfigOpType['SET'])
                        for name, value in config_dicts.items()]
        
        # Create ConfigResource with the list of configurations
        resource = ConfigResource('topic', topic_name, incremental_configs=config_entries)
        
        result_dict = admin_client.incremental_alter_configs([resource])
        result_dict[resource].result()  # Wait for the result to ensure the configuration is applied
    except KafkaException as e:
        if e.args and e.args[0].code() == KafkaError._UNKNOWN_TOPIC_OR_PART:
            print(f"Kafka error occurred: The topic '{topic_name}' does not exist.")
        else:
            print(f"Kafka error occurred: {e}")
        raise

def main():

    # Configura o parser de argumentos
    parser = argparse.ArgumentParser(description='Atualiza a configuração de um tópico do Kafka.')
    parser.add_argument('topic_name', type=str, help='Nome do tópico')
    
    # Recebe os parâmetros da linha de comando
    args = parser.parse_args()
    topic_name = args.topic_name
    
    # Configuração do cliente Kafka
    admin_client = AdminClient({'bootstrap.servers': '13.92.98.80:9092'})
    
    # Define configurações para serem aplicadas
    config_dicts = {
        'retention.ms': 10,
        'cleanup.policy': 'compact'
    }
    try:
        # Define múltiplas configurações
        set_config(admin_client, topic_name, config_dicts)
        
        # Verifica se as propriedades de configuração foram atualizadas
        for config_name in config_dicts.keys():
            new_value = get_config(admin_client, topic_name, config_name)
            print(f'Now {config_name} for topic {topic_name} is {new_value}')

        sys.exit(0)
    except KafkaException as e:
        print(f"Kafka error occurred: {e}")
        sys.exit(1)
    except ValueError as e:
        print(f"Value error occurred: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()