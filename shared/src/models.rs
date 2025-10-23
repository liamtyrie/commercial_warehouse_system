
#[derive(Debug, PartialEq)]
pub struct StockReceivedEvent {
    pub product_id: String,
    pub quantity: u32,
    pub warehouse_location: String,
    pub received_by: String,
    pub received_at: String,
}

pub struct ReceiveStockCommand {
    pub product_id: String,
    pub quantity: u32,
    pub warehouse_location: String,
    pub received_by: String,
}

#[derive(Debug, PartialEq)]
pub enum FulfillmentError {
    InvalidQuantity,
    InvalidProductId,
    InvalidWarehouseLocation,
}