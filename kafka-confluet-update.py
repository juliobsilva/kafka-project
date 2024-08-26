from confluent_kafka.admin import AdminClient, ConfigResource, ConfigEntry, AlterConfigOpType

def get_config(topic_name, config_name):
    admin_client = AdminClient({'bootstrap.servers': '13.92.98.80:9092'})
    resource = ConfigResource('topic', topic_name)
    result_dict = admin_client.describe_configs([resource])
    config_entries = result_dict[resource].result()
    if config_name in config_entries:
        config_property = config_entries[config_name]
        return config_property.value
    else:
        raise ValueError(f"Configuration {config_name} not found for topic {topic_name}")

def set_config(topic_name, config_dicts):
    admin_client = AdminClient({'bootstrap.servers': '13.92.98.80:9092'})
    
    # Create ConfigEntry objects for each config to set
    config_entries = [ConfigEntry(name=name, value=str(value), incremental_operation=AlterConfigOpType['SET'])
                      for name, value in config_dicts.items()]
    
    # Create ConfigResource with the list of configurations
    resource = ConfigResource('topic', topic_name, incremental_configs=config_entries)
    
    result_dict = admin_client.incremental_alter_configs([resource])
    result_dict[resource].result()

if __name__ == '__main__':
    topic_name = 'j-d-g-r'
    
    # Define configurations to be set
    config_dicts = {
        'retention.ms': 10,
        'cleanup.policy': 'compact'
    }
    
    # Set multiple configurations
    set_config(topic_name, config_dicts)
    
    # Check if the config property has been updated
    for config_name in config_dicts.keys():
        new_value = get_config(topic_name, config_name)
        print(f'Now {config_name} for topic {topic_name} is {new_value}')

