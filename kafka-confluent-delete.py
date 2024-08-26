import sys
import argparse
from confluent_kafka.admin import AdminClient

def delete_topic(admin_client, topic_name):
    # Deletar o tópico
    try:
        # Verifica se o tópico já existe
        metadata = admin_client.list_topics(timeout=10)
        if topic_name not in metadata.topics:
            print(f"O tópico '{topic_name}' não existe.")
            return 1
        
        admin_client.delete_topics([topic_name])
        print(f'Tópico "{topic_name}" deletado com sucesso.')
    except Exception as e:
        print(f'Erro ao deletar o tópico: {e}')

def main():
    # Configuração do cliente AdminClient
    admin_client = AdminClient({'bootstrap.servers': '13.92.98.80:9092'})
    
    # Configura o parser de argumentos
    parser = argparse.ArgumentParser(description='Deleta um tópico do Kafka.')
    parser.add_argument('topic_name', type=str, help='Nome do tópico a ser deletado')

    args = parser.parse_args()

    # Recebe o nome do tópico da linha de comando
    topic_name = args.topic_name

    if not topic_name:
        parser.error('O parâmetro "topic_name" é obrigatório.')
    
    # Chama a função para deletar o tópico
    exit_code = delete_topic(admin_client, topic_name)
    sys.exit(exit_code)