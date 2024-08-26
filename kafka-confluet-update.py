import argparse
from confluent_kafka.admin import AdminClient, ConfigResource, ConfigEntry, AlterConfigOpType

def get_config(admin_client, topic_name, config_name):
    resource = ConfigResource('topic', topic_name)
    result_dict = admin_client.describe_configs([resource])
    config_entries = result_dict[resource].result()
    if config_name in config_entries:
        config_property = config_entries[config_name]
        return config_property.value
    else:
        raise ValueError(f"Configuration {config_name} not found for topic {topic_name}")

def set_config(admin_client, topic_name, config_dicts):
    # Create ConfigEntry objects for each config to set
    config_entries = [ConfigEntry(name=name, value=str(value), incremental_operation=AlterConfigOpType['SET'])
                      for name, value in config_dicts.items()]
    
    # Create ConfigResource with the list of configurations
    resource = ConfigResource('topic', topic_name, incremental_configs=config_entries)
    
    result_dict = admin_client.incremental_alter_configs([resource])
    result_dict[resource].result()  # Wait for the result to ensure the configuration is applied

def main():
    # Configura o parser de argumentos
    parser = argparse.ArgumentParser(description='Atualiza a configuração de um tópico do Kafka.')
    parser.add_argument('topic_name', type=str, help='Nome do tópico')
    
    # Recebe os parâmetros da linha de comando
    args = parser.parse_args()
    topic_name = args.topic_name
    
    # Configuração do cliente Kafka
    admin_client = admin_client({'bootstrap.servers': '13.92.98.80:9092'})
    
    # Define configurações para serem aplicadas
    config_dicts = {
        'retention.ms': 10,
        'cleanup.policy': 'compact'
    }
    
    # Define múltiplas configurações
    set_config(admin_client, topic_name, config_dicts)
    
    # Verifica se as propriedades de configuração foram atualizadas
    for config_name in config_dicts.keys():
        new_value = get_config(admin_client, topic_name, config_name)
        print(f'Now {config_name} for topic {topic_name} is {new_value}')

if __name__ == '__main__':
    main()