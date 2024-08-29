import argparse
from confluent_kafka.admin import AdminClient, AclBinding, AclOperation, AclPermissionType, ResourceType, ResourcePatternType, AclBindingFilter

def set_permission_topic(admin_client, topic_name, user_name):
    # Definindo ACL para um tópico específico
    acl_read = AclBinding(
        restype=ResourceType.TOPIC,
        name=topic_name,
        resource_pattern_type=ResourcePatternType.LITERAL,  
        principal=f'User:{user_name}',
        host="*",
        operation=AclOperation.READ, 
        permission_type=AclPermissionType.ALLOW
    )

    # Cria as ACLs no Kafka
    try:
        futures = admin_client.create_acls([acl_read])
        for acl, future in futures.items():
            try:
                future.result()
                print(f"Permissão {acl.operation} concedida com sucesso para {acl.principal} no tópico {acl.name}")
            except Exception as e:
                print(f"Falha ao conceder permissão em {acl.operation}: {e}")
    except Exception as e:
        print(f"Erro ao criar ACLs: {e}")

def check_acl(admin_client, topic_name, user_name):
    # Definindo o filtro para descrever ACLs
    acl_filter = AclBindingFilter(
        restype=ResourceType.TOPIC,
        name=topic_name,
        resource_pattern_type=ResourcePatternType.LITERAL,
        principal=f'User:{user_name}',
        host='*',
        operation=AclOperation.READ,
        permission_type=AclPermissionType.ALLOW
    )

    try:
        acls = admin_client.describe_acls([acl_filter])
        for acl in acls:
            print(f"ACL encontrada: {acl}")
    except Exception as e:
        print(f"Erro ao descrever ACLs: {e}")

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
    admin_client = AdminClient({'bootstrap.servers': 'pkc-12576z.us-west2.gcp.confluent.cloud:9092',
                                'security.protocol': 'SASL_SSL',
                                'sasl.mechanisms':'PLAIN',
                                'sasl.username': 'VWIFLOJGPI33ZBOO',
                                'sasl.password': '+F0MrPFaRvTqaIfKqYhn99x8yKZrM+ZXtvDoM6Tjd6I7qMs/cpqXXbAkMNGTTZlB'
                                })

    # Chama a função para conceder permissões
    set_permission_topic(admin_client, topic_name, user_name)
    check_acl(admin_client, topic_name, user_name)

if __name__ == "__main__":
    main()
