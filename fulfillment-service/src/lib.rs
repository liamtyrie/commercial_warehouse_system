use shared::models::*;



pub fn handle_receive_stock(command: ReceiveStockCommand) -> Result<StockReceivedEvent, FulfillmentError> {
    if command.quantity == 0 {
        return Err(FulfillmentError::InvalidQuantity)
    }

    Ok(StockReceivedEvent { product_id: command.product_id, quantity: command.quantity, warehouse_location: command.warehouse_location, received_by: command.received_by, received_at: chrono::Utc::now().to_rfc3339(), })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_produce_stock_received_event_from_valid_command() {
        let command = ReceiveStockCommand {
            product_id: "PROD-123".to_string(),
            quantity: 100,
            warehouse_location: "AMZ-331-BAY-1".to_string(),
            received_by: "WMS-1138".to_string(),
        };

        let result = handle_receive_stock(command);
        assert!(result.is_ok());

        let event = result.unwrap();
        assert_eq!(event.product_id, "PROD-123");
        assert_eq!(event.quantity, 100);
        assert_eq!(event.received_by, "WMS-1138");
    }

    #[test]
    fn should_return_error_for_zero_quantity_command() {
        let command = ReceiveStockCommand {
            product_id: "PROD-123".to_string(),
            quantity: 0,
            warehouse_location: "AMZ-331-BAY-1".to_string(),
            received_by: "WMS-1138".to_string(),
        };

        let result = handle_receive_stock(command);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), FulfillmentError::InvalidQuantity);
    }
}