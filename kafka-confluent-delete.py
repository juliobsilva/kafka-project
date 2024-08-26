import sys
import argparse
from confluent_kafka.admin import AdminClient
from confluent_kafka.error import KafkaException

def delete_topic(admin_client, topic_name):
    try:
        # Verifica se o tópico já existe
        metadata = admin_client.list_topics(timeout=10)
        if topic_name not in metadata.topics:
            print(f'O tópico "{topic_name}" não existe.')
            return 1
        
        # Deletar o tópico
        futures = admin_client.delete_topics([topic_name])
        
        # Aguarde a conclusão da deleção
        for topic, future in futures.items():
            try:
                future.result()  # Bloqueia até a deleção ser concluída
                print(f'Tópico "{topic}" deletado com sucesso.')
            except KafkaException as e:
                print(f'Erro ao deletar o tópico "{topic}": {e}')
                return 1
        
        return 0
    except Exception as e:
        print(f'Erro ao deletar o tópico: {e}')
        return 1

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

if __name__ == '__main__':
    main()