from datetime import datetime

from pydantic import BaseModel, Field

class ProductSchema(BaseModel):
    name: str = Field(min_length=1, max_length=50, description='Product name')
    price: float = Field(gt=0, lt=100_000, description="Price of the product")
    quantity: int = Field(gt=0, lt=50, description="Quantity of the product")

class ProductRead(ProductSchema):
    id: int
    created_at: datetime