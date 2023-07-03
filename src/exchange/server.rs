use std::collections::HashMap;
use std::result::Result;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{transport::Server, Request, Response, Status};

pub mod exchange {
    tonic::include_proto!("exchange");
}

use exchange::exchange_server::{Exchange, ExchangeServer};
use exchange::{Order, OrderId, Void};

pub struct MyExchange {
    orderbook: Arc<Mutex<HashMap<u64, Order>>>,
    last: Arc<Mutex<u64>>,
    last_seen_quote: Arc<Mutex<HashMap<String, i64>>>,
}

#[tonic::async_trait]
impl Exchange for MyExchange {
    async fn insert_order(&self, order: Request<Order>) -> Result<Response<OrderId>, Status> {
        let mut last = self.last.lock().await;
        let mut orderbook = self.orderbook.lock().await;
        orderbook.insert(*last, order.into_inner());

        let oid = OrderId { order_id: last.clone() };

        *last +=1;
        Ok(Response::new(oid))
    }

    async fn delete_order(&self, order: Request<OrderId>) -> Result<Response<Void>, Status> {
        let mut orderbook = self.orderbook.lock().await;
        orderbook.remove(&order.into_inner().order_id);
        Ok(Response::new(Void {}))
    }

    async fn get_order(&self, order: Request<OrderId>) -> Result<Response<Order>, Status> {
        let orderbook = self.orderbook.lock().await;
        if let Some(order) = orderbook.get(&order.into_inner().order_id) {
            Ok(Response::new(order.clone()))
        } else {
            Err(Status::not_found("Not found"))
        }
    }

    async fn check(&self, order: Request<Void>) -> Result<Response<Void>, Status> {
        Ok(Response::new(Void {}))
    }

    async fn finish(&self, order: Request<Void>) -> Result<Response<Void>, Status> {
        Ok(Response::new(Void {}))
    }
}

impl Default for MyExchange {
    fn default() -> Self {
        Self {
            last: Arc::new(Mutex::new(0)),
            orderbook: Arc::new(Mutex::new(HashMap::new())),
            last_seen_quote: Arc::new(Mutex::new(HashMap::new())),
        }
    }

}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse().unwrap();
    let greeter = MyExchange::default();

    println!("ExchangeServer listening on {}", addr);

    Server::builder()
        .add_service(ExchangeServer::new(greeter))
        .serve(addr)
        .await?;

    Ok(())
}
