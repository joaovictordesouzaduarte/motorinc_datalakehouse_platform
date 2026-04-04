import psycopg2
import random
import time
from datetime import datetime, timedelta
import logging
import os
from dotenv import load_dotenv

load_dotenv()
# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# PostgreSQL database connection settings
DB_CONFIG = {
    'host': os.getenv('AWS_DB_HOST'),
    'database': 'motorinc_oltp',
    'user': os.getenv('AWS_DB_USER'),
    'password': os.getenv('AWS_DB_PASSWORD'),
    'port': os.getenv('AWS_DB_PORT'),
}

# Possible values for customernumber
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

# Possible values for productcode and priceeach
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

# Default comment values for the comments field
COMMENTS = [
    "Customer requested gift wrapping.",
    "Priority delivery requested.",
    "Returning customer, check possibility of discount on future purchases.",
    "Delivery to commercial address.",
    "Customer requested contact before delivery.",
    "Payment confirmed, release for immediate shipping.",
    "Check stock availability before confirming order.",
    "Customer requested invoice in company name.",
    "Awaiting payment confirmation for processing.",
    "Delivery to gated community, notify reception.",
    "Collector's item, verify packaging quality.",
    "Customer requested additional shipping insurance.",
    "Deliver only to the recipient.",
    "VIP customer, prioritize service.",
    None  # Order may have no comment
]

class OrderGenerator:
    def __init__(self, connection_params):
        self.conn = None
        self.connection_params = connection_params
        self.current_order_number = None  # Will be set in the generate_order method
    
    def connect(self):
        """Establishes a connection to the database"""
        try:
            self.conn = psycopg2.connect(**self.connection_params)
            logger.info("Database connection established.")
            return True
        except Exception as e:
            logger.error(f"Error connecting to the database: {e}")
            return False
    
    def disconnect(self):
        """Closes the database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed.")
    
    def _get_next_order_number(self):
        """Retrieves the next available order number"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT MAX(ordernumber) FROM public.orders")
            result = cursor.fetchone()
            cursor.close()
            
            if result[0] is not None:
                # Adds 1 to the highest order number found
                return result[0] + 30
            return 10000  # Initial value if the table is empty
        except Exception as e:
            logger.error(f"Error retrieving next order number: {e}")
            return 10000
    
    def _is_order_number_available(self, order_number):
        """Checks if an order number already exists in the database"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM public.orders WHERE ordernumber = %s", (order_number,))
            result = cursor.fetchone()
            cursor.close()
            
            # If the count is 0, the number is available
            return result[0] == 0
        except Exception as e:
            logger.error(f"Error checking order number availability: {e}")
            return False
    
    def generate_order(self):
        """Generates a new order and its details"""
        try:
            if not self.connect():
                return False
            
            # Gets the next available order number
            next_order_number = self._get_next_order_number()
            
            # Verifies the number is truly available to ensure uniqueness
            while not self._is_order_number_available(next_order_number):
                logger.warning(f"Order number {next_order_number} already exists. Trying the next one.")
                next_order_number += 1
            
            # Sets the order number
            self.current_order_number = next_order_number
            
            # Order information
            order_date = datetime.now()
            # Setting the required date with the time part zeroed out
            required_date = (order_date + timedelta(days=random.randint(5, 10))).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            # The shippeddate field must always be null
            shipped_date = None
            # The status field must always be "In Process"
            status = "In Process"
            comments = random.choice(COMMENTS)
            customer_number = random.choice(CUSTOMER_NUMBERS)
            
            # Insert the order into the orders table
            cursor = self.conn.cursor()
            cursor.execute(
                """
                INSERT INTO public.orders 
                (ordernumber, orderdate, requireddate, shippeddate, status, comments, customernumber)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (self.current_order_number, order_date, required_date, shipped_date, status, comments, customer_number)
            )
            
            # Number of items in the order (between 1 and 5)
            num_items = random.randint(1, 5)
            
            # Selects random products for the order (without repetition)
            selected_products = random.sample(list(PRODUCT_PRICES.keys()), min(num_items, len(PRODUCT_PRICES)))
            
            # Insert the order details
            for index, product_code in enumerate(selected_products, 1):
                quantity = random.randint(1, 5)
                price_each = PRODUCT_PRICES[product_code]
                order_line_number = index  # Sequential number for each order line
                
                cursor.execute(
                    """
                    INSERT INTO public.orderdetails
                    (ordernumber, productcode, quantityordered, priceeach, orderlinenumber)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (self.current_order_number, product_code, quantity, price_each, order_line_number)
                )
            
            # Commits the transaction
            self.conn.commit()
            cursor.close()
            
            logger.info(f"Order #{self.current_order_number} successfully generated with {len(selected_products)} products.")
            return True
        
        except Exception as e:
            if self.conn:
                self.conn.rollback()
            logger.error(f"Error generating order: {e}")
            return False
        
        finally:
            self.disconnect()

def run_order_generator():
    """Runs the order generator continuously"""
    generator = OrderGenerator(DB_CONFIG)
    
    logger.info("Starting the order generator...")
    
    try:
        while True:
            # Generates a new order
            success = generator.generate_order()
            
            if success:
                # Waits a random time between 20 and 60 seconds
                wait_time = random.randint(15, 30)
                logger.info(f"Waiting {wait_time} seconds until the next order...")
                time.sleep(wait_time)
            else:
                # If there is a failure, waits 30 seconds before retrying
                logger.warning("Failed to generate order. Retrying in 30 seconds...")
                time.sleep(3)
    
    except KeyboardInterrupt:
        logger.info("Order generator stopped by the user.")

if __name__ == "__main__":
    run_order_generator()
