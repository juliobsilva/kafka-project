import argparse
from confluent_kafka.admin import AdminClient, ConfigResource, ConfigEntry, AlterConfigOpType, NewTopic, AclBinding,AclBinding, AclOperation, AclPermissionType, ResourceType

def set_permission_topic(admin_client, topic_name, user_name):
    
    topic_name = topic_name
    user_name = user_name

    # Definindo ACL para um tópico específico
    acl_read = AclBinding (
        restype=ResourceType.TOPIC,
        name='{topic_name}',
        resource_pattern_type=None,  # Pode deixar como None ou configurar conforme necessário
        principal="User:{user_name}",
        host="*",
        operation=AclOperation.READ,  # Ou WRITE, ALL, etc.
        permission_type=AclPermissionType.ALLOW
    )

    # Cria as ACLs no Kafka
    futures = admin_client.create_acls([acl_read])

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