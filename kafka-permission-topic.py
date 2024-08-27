import argparse
from confluent_kafka.admin import AdminClient, AclBinding, ResourcePattern, AclOperation, AclPermissionType, ResourceType

def set_permission_topic(admin_client, topic_name, user_name):
    
    topic_name = topic_name
    user_name = user_name

    # Define a permissão de leitura
    acl_read = AclBinding(
        ResourcePattern(ResourceType.TOPIC, topic_name, ResourcePattern.ResourcePatternType.LITERAL),
        user_name,
        '*',  # Host pode ser especificado conforme necessário
        AclOperation.READ,
        AclPermissionType.ALLOW
    )

    # Define a permissão de escrita
    acl_write = AclBinding(
        ResourcePattern(ResourceType.TOPIC, topic_name, ResourcePattern.ResourcePatternType.LITERAL),
        user_name,
        '*',  # Host pode ser especificado conforme necessário
        AclOperation.WRITE,
        AclPermissionType.ALLOW
    )

    # Cria as ACLs no Kafka
    futures = admin_client.create_acls([acl_read, acl_write])

    # Verifica se as ACLs foram criadas com sucesso
    for acl, future in futures.items():
        try:
            future.result()
            print(f"Permissão {acl.operation} concedida com sucesso para {acl.principal} no tópico {acl.resource.pattern}")
        except Exception as e:
            print(f"Falha ao conceder permissão {acl.operation}: {e}")

def main():
    # Configura o parser de argumentos
    parser = argparse.ArgumentParser(description='Concede permissões de leitura e escrita em um tópico do Kafka.')
    parser.add_argument('topic_name', type=str, help='Nome do tópico')
    parser.add_argument('user_name', type=str, help='Nome do usuário')

    args = parser.parse_args()

    # Recebe os parâmetros da linha de comando

    topic_name = args.topic_name
    user_name = args.user_name

    # Configuração do cliente AdminClient
    admin_client = AdminClient({'bootstrap.servers': '13.92.98.80:9092'})

    # Chama a função para conceder permissões
    set_permission_topic(admin_client, topic_name, user_name)