import psycopg2
import random
import time
from datetime import datetime, timedelta
import logging

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configurações de conexão com o banco de dados PostgreSQL
DB_CONFIG = {
    'host': os.getenv('AWS_DB_HOST'),
    'database': 'motorinc_oltp',
    'user': os.getenv('AWS_DB_USER'),
    'password': os.getenv('AWS_DB_PASSWORD'),
    'port': os.getenv('AWS_DB_PORT'),
}

# Valores possíveis para customernumber
CUSTOMER_NUMBERS = [
    103, 112, 114, 119, 121, 124, 128, 129, 131,
    141, 144, 145, 146, 148, 151, 157, 161, 166,
    167, 168, 171, 172, 173, 175, 177, 181, 186,
    187, 189, 198, 201, 202, 204, 205, 209, 211,
    216, 219, 227, 233, 239, 240, 242, 249, 250,
    256, 259, 260, 276, 278, 282, 286, 298, 299,
    311, 314, 319, 320, 321, 323, 324, 328, 333,
    334, 339, 344, 347, 350, 353, 357, 362, 363,
    376, 379, 381, 382, 385, 386, 398, 406, 412,
    415, 424, 447, 448, 450, 452, 455, 456, 458,
    462, 471, 473, 475, 484, 486, 487, 489, 495,
    496
]

# Valores possíveis para productcode e priceeach
PRODUCT_PRICES = {
    1514: 69.00,
    2011: 147.00,
    2824: 116.00,
    2834: 69.00,
    2972: 45.00,
    3212: 44.00,
    3320: 73.00,
    3891: 183.00,
    3962: 114.00,
    4675: 122.00
}

# Comentários padrão para o campo comments
COMMENTS = [
    "Cliente solicitou embalagem para presente.",
    "Entrega prioritária solicitada.",
    "Cliente é recorrente, verificar possibilidade de desconto em compras futuras.",
    "Entrega em endereço comercial.",
    "Cliente solicitou contato antes da entrega.",
    "Pagamento confirmado, liberar envio imediato.",
    "Verificar disponibilidade de estoque antes de confirmar pedido.",
    "Cliente solicitou nota fiscal em nome da empresa.",
    "Aguardando confirmação de pagamento para processamento.",
    "Entrega em condomínio, avisar portaria.",
    "Produto para colecionador, verificar qualidade da embalagem.",
    "Cliente solicitou seguro adicional para o envio.",
    "Entregar somente ao destinatário.",
    "Cliente é VIP, priorizar atendimento.",
    None  # Possibilidade de não ter comentário
]

class OrderGenerator:
    def __init__(self, connection_params):
        self.conn = None
        self.connection_params = connection_params
        self.current_order_number = None  # Será definido no método generate_order
    
    def connect(self):
        """Estabelece conexão com o banco de dados"""
        try:
            self.conn = psycopg2.connect(**self.connection_params)
            logger.info("Conexão estabelecida com o banco de dados.")
            return True
        except Exception as e:
            logger.error(f"Erro ao conectar ao banco de dados: {e}")
            return False
    
    def disconnect(self):
        """Encerra a conexão com o banco de dados"""
        if self.conn:
            self.conn.close()
            logger.info("Conexão com o banco de dados encerrada.")
    
    def _get_next_order_number(self):
        """Recupera o próximo número de pedido disponível"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT MAX(ordernumber) FROM public.orders")
            result = cursor.fetchone()
            cursor.close()
            
            if result[0] is not None:
                # Adiciona 1 ao maior número de pedido encontrado
                return result[0] + 30
            return 10000  # Valor inicial se a tabela estiver vazia
        except Exception as e:
            logger.error(f"Erro ao recuperar próximo número de pedido: {e}")
            return 10000
    
    def _is_order_number_available(self, order_number):
        """Verifica se um número de pedido já existe no banco de dados"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM public.orders WHERE ordernumber = %s", (order_number,))
            result = cursor.fetchone()
            cursor.close()
            
            # Se o contador for 0, o número está disponível
            return result[0] == 0
        except Exception as e:
            logger.error(f"Erro ao verificar disponibilidade do número de pedido: {e}")
            return False
    
    def generate_order(self):
        """Gera um novo pedido e seus detalhes"""
        try:
            if not self.connect():
                return False
            
            # Obtém o próximo número de pedido disponível
            next_order_number = self._get_next_order_number()
            
            # Verifica se o número está realmente disponível para garantir
            while not self._is_order_number_available(next_order_number):
                logger.warning(f"Número de pedido {next_order_number} já existe. Tentando o próximo.")
                next_order_number += 1
            
            # Define o número do pedido
            self.current_order_number = next_order_number
            
            # Informações do pedido
            order_date = datetime.now()
            # Definindo a data requerida com a parte do horário zerada
            required_date = (order_date + timedelta(days=random.randint(5, 10))).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            # O campo shippeddate deve ser sempre nulo
            shipped_date = None
            # O campo status deve ser sempre "In Process"
            status = "In Process"
            comments = random.choice(COMMENTS)
            customer_number = random.choice(CUSTOMER_NUMBERS)
            
            # Inserir o pedido na tabela orders
            cursor = self.conn.cursor()
            cursor.execute(
                """
                INSERT INTO public.orders 
                (ordernumber, orderdate, requireddate, shippeddate, status, comments, customernumber)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (self.current_order_number, order_date, required_date, shipped_date, status, comments, customer_number)
            )
            
            # Número de itens no pedido (entre 1 e 5)
            num_items = random.randint(1, 5)
            
            # Seleciona produtos aleatórios para o pedido (sem repetição)
            selected_products = random.sample(list(PRODUCT_PRICES.keys()), min(num_items, len(PRODUCT_PRICES)))
            
            # Inserir os detalhes do pedido
            for index, product_code in enumerate(selected_products, 1):
                quantity = random.randint(1, 5)
                price_each = PRODUCT_PRICES[product_code]
                order_line_number = index  # Número sequencial para cada linha do pedido
                
                cursor.execute(
                    """
                    INSERT INTO public.orderdetails
                    (ordernumber, productcode, quantityordered, priceeach, orderlinenumber)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (self.current_order_number, product_code, quantity, price_each, order_line_number)
                )
            
            # Confirma a transação
            self.conn.commit()
            cursor.close()
            
            logger.info(f"Pedido #{self.current_order_number} gerado com sucesso com {len(selected_products)} produtos.")
            return True
        
        except Exception as e:
            if self.conn:
                self.conn.rollback()
            logger.error(f"Erro ao gerar pedido: {e}")
            return False
        
        finally:
            self.disconnect()

def run_order_generator():
    """Executa o gerador de pedidos continuamente"""
    generator = OrderGenerator(DB_CONFIG)
    
    logger.info("Iniciando o gerador de pedidos...")
    
    try:
        while True:
            # Gera um novo pedido
            success = generator.generate_order()
            
            if success:
                # Aguarda um tempo aleatório entre 10 e 20 segundos
                wait_time = random.randint(20, 60)
                logger.info(f"Aguardando {wait_time} segundos para o próximo pedido...")
                time.sleep(wait_time)
            else:
                # Se houver falha, aguarda 30 segundos antes de tentar novamente
                logger.warning("Falha ao gerar pedido. Tentando novamente em 30 segundos...")
                time.sleep(3)
    
    except KeyboardInterrupt:
        logger.info("Gerador de pedidos interrompido pelo usuário.")

if __name__ == "__main__":
    run_order_generator()