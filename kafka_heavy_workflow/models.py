from pydantic import BaseModel
from typing import Optional

class orders(BaseModel):
    order_id: int
    order_date: str
    order_customer_id: int
    order_status: str

class order_items(BaseModel):
    order_item_id: int
    order_item_order_id: int
    order_item_product_id: int
    order_item_quantity: int
    order_item_subtotal: float
    order_item_product_price: float

class categories(BaseModel):
    category_id: int
    category_department_id: int
    category_name: str

class departments(BaseModel):
    department_id: int
    department_name: str

class products(BaseModel):
    product_id: str
    product_category_id: int
    product_name: str
    product_description: Optional[str] = None
    product_price: float
    product_image: Optional[str] = None

class customers(BaseModel):
    customer_id: int
    customer_fname: str
    customer_lname: str
    customer_email: str
    customer_password: str
    customer_street: str
    customer_city: str
    customer_state: str
    customer_zipcode: int