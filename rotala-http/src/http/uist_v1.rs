use std::collections::HashMap;
use std::future::Future;
use std::sync::Mutex;

use anyhow::Result;
use serde::{Deserialize, Serialize};

use rotala::exchange::uist_v1::{Order, OrderId, Trade, UistV1};
use rotala::input::penelope::{Penelope, PenelopeQuoteByDate};

pub type BacktestId = u64;

pub struct BacktestState {
    pub id: BacktestId,
    pub date: i64,
    pub pos: usize,
    pub exchange: UistV1,
    pub dataset_name: String,
}

pub struct AppState {
    pub backtests: HashMap<BacktestId, BacktestState>,
    pub last: BacktestId,
    pub datasets: HashMap<String, Penelope>,
}

impl AppState {
    pub fn create(datasets: &mut HashMap<String, Penelope>) -> Self {
        Self {
            backtests: HashMap::new(),
            last: 0,
            datasets: std::mem::take(datasets),
        }
    }

    pub fn single(name: &str, data: Penelope) -> Self {
        let exchange = UistV1::new();
        let backtest = BacktestState {
            id: 0,
            date: *data.get_date(0).unwrap(),
            pos: 0,
            exchange,
            dataset_name: name.into(),
        };

        let mut datasets = HashMap::new();
        datasets.insert(name.into(), data);

        let mut backtests = HashMap::new();
        backtests.insert(0, backtest);

        Self {
            backtests,
            last: 1,
            datasets,
        }
    }

    pub fn tick(&mut self, backtest_id: BacktestId) -> Option<(bool, Vec<Trade>, Vec<Order>)> {
        if let Some(backtest) = self.backtests.get_mut(&backtest_id) {
            if let Some(dataset) = self.datasets.get(&backtest.dataset_name) {
                let mut has_next = false;
                let mut executed_trades = Vec::new();
                let mut inserted_orders = Vec::new();

                if let Some(quotes) = dataset.get_quotes(&backtest.date) {
                    let mut res = backtest.exchange.tick(quotes);
                    executed_trades.append(&mut res.0);
                    inserted_orders.append(&mut res.1);
                }

                let new_pos = backtest.pos + 1;
                if dataset.has_next(new_pos) {
                    has_next = true;
                    backtest.date = *dataset.get_date(new_pos).unwrap();
                }
                backtest.pos = new_pos;
                return Some((has_next, executed_trades, inserted_orders));
            }
        }
        None
    }

    pub fn fetch_quotes(&self, backtest_id: BacktestId) -> Option<&PenelopeQuoteByDate> {
        if let Some(backtest) = self.backtests.get(&backtest_id) {
            if let Some(dataset) = self.datasets.get(&backtest.dataset_name) {
                return dataset.get_quotes(&backtest.date);
            }
        }
        None
    }

    pub fn init(&mut self, dataset_name: String) -> Option<BacktestId> {
        if let Some(dataset) = self.datasets.get(&dataset_name) {
            let new_id = self.last + 1;
            let exchange = UistV1::new();
            let backtest = BacktestState {
                id: new_id,
                date: *dataset.get_date(0).unwrap(),
                pos: 0,
                exchange,
                dataset_name,
            };
            self.backtests.insert(new_id, backtest);
            return Some(new_id);
        }
        None
    }

    pub fn insert_order(&mut self, order: Order, backtest_id: BacktestId) -> Option<()> {
        if let Some(backtest) = self.backtests.get_mut(&backtest_id) {
            backtest.exchange.insert_order(order);
            return Some(());
        }
        None
    }

    pub fn delete_order(&mut self, order_id: OrderId, backtest_id: BacktestId) -> Option<()> {
        if let Some(backtest) = self.backtests.get_mut(&backtest_id) {
            backtest.exchange.delete_order(order_id);
            return Some(());
        }
        None
    }

    pub fn new_backtest(&mut self, dataset_name: &str) -> Option<BacktestId> {
        let new_id = self.last + 1;

        // Check that dataset exists
        if let Some(dataset) = self.datasets.get(dataset_name) {
            let exchange = UistV1::new();

            let backtest = BacktestState {
                id: new_id,
                date: *dataset.get_date(0).unwrap(),
                pos: 0,
                exchange,
                dataset_name: dataset_name.into(),
            };

            self.backtests.insert(new_id, backtest);

            self.last = new_id;
            return Some(new_id);
        }
        None
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TickResponse {
    pub has_next: bool,
    pub executed_trades: Vec<Trade>,
    pub inserted_orders: Vec<Order>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct InsertOrderRequest {
    pub order: Order,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct DeleteOrderRequest {
    pub order_id: OrderId,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FetchQuotesResponse {
    pub quotes: PenelopeQuoteByDate,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct InitResponse {
    pub backtest_id: BacktestId,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct InfoResponse {
    pub version: String,
    pub dataset: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct NowResponse {
    pub now: i64,
    pub has_next: bool,
}

#[derive(Debug)]
pub enum UistV1Error {
    UnknownBacktest,
    UnknownDataset,
}

impl std::error::Error for UistV1Error {}

impl core::fmt::Display for UistV1Error {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            UistV1Error::UnknownBacktest => write!(f, "UnknownBacktest"),
            UistV1Error::UnknownDataset => write!(f, "UnknownDataset"),
        }
    }
}

impl actix_web::ResponseError for UistV1Error {
    fn status_code(&self) -> actix_web::http::StatusCode {
        match self {
            UistV1Error::UnknownBacktest => actix_web::http::StatusCode::BAD_REQUEST,
            UistV1Error::UnknownDataset => actix_web::http::StatusCode::BAD_REQUEST,
        }
    }
}

pub trait Client {
    fn tick(&mut self, backtest_id: BacktestId) -> impl Future<Output = Result<TickResponse>>;
    fn delete_order(
        &mut self,
        order_id: OrderId,
        backtest_id: BacktestId,
    ) -> impl Future<Output = Result<()>>;
    fn insert_order(
        &mut self,
        order: Order,
        backtest_id: BacktestId,
    ) -> impl Future<Output = Result<()>>;
    fn fetch_quotes(
        &mut self,
        backtest_id: BacktestId,
    ) -> impl Future<Output = Result<FetchQuotesResponse>>;
    fn init(&mut self, dataset_name: String) -> impl Future<Output = Result<InitResponse>>;
    fn info(&mut self, backtest_id: BacktestId) -> impl Future<Output = Result<InfoResponse>>;
    fn now(&mut self, backtest_id: BacktestId) -> impl Future<Output = Result<NowResponse>>;
}

type UistState = Mutex<AppState>;

pub mod server {
    use actix_web::{get, post, web};

    use super::{
        BacktestId, DeleteOrderRequest, FetchQuotesResponse, InfoResponse, InitResponse,
        InsertOrderRequest, NowResponse, TickResponse, UistState, UistV1Error,
    };

    #[get("/backtest/{backtest_id}/tick")]
    pub async fn tick(
        app: web::Data<UistState>,
        path: web::Path<(BacktestId,)>,
    ) -> Result<web::Json<TickResponse>, UistV1Error> {
        let mut uist = app.lock().unwrap();
        let (backtest_id,) = path.into_inner();

        if let Some(result) = uist.tick(backtest_id) {
            Ok(web::Json(TickResponse {
                inserted_orders: result.2,
                executed_trades: result.1,
                has_next: result.0,
            }))
        } else {
            Err(UistV1Error::UnknownBacktest)
        }
    }

    #[post("/backtest/{backtest_id}/delete_order")]
    pub async fn delete_order(
        app: web::Data<UistState>,
        path: web::Path<(BacktestId,)>,
        delete_order: web::Json<DeleteOrderRequest>,
    ) -> Result<web::Json<()>, UistV1Error> {
        let mut uist = app.lock().unwrap();
        let (backtest_id,) = path.into_inner();

        if let Some(()) = uist.delete_order(delete_order.order_id, backtest_id) {
            Ok(web::Json(()))
        } else {
            Err(UistV1Error::UnknownBacktest)
        }
    }

    #[post("/backtest/{backtest_id}/insert_order")]
    pub async fn insert_order(
        app: web::Data<UistState>,
        path: web::Path<(BacktestId,)>,
        insert_order: web::Json<InsertOrderRequest>,
    ) -> Result<web::Json<()>, UistV1Error> {
        let mut uist = app.lock().unwrap();
        let (backtest_id,) = path.into_inner();
        if let Some(()) = uist.insert_order(insert_order.order.clone(), backtest_id) {
            Ok(web::Json(()))
        } else {
            Err(UistV1Error::UnknownBacktest)
        }
    }

    #[get("/backtest/{backtest_id}/fetch_quotes")]
    pub async fn fetch_quotes(
        app: web::Data<UistState>,
        path: web::Path<(BacktestId,)>,
    ) -> Result<web::Json<FetchQuotesResponse>, UistV1Error> {
        let uist = app.lock().unwrap();
        let (backtest_id,) = path.into_inner();

        if let Some(quotes) = uist.fetch_quotes(backtest_id) {
            Ok(web::Json(FetchQuotesResponse {
                quotes: quotes.clone(),
            }))
        } else {
            Err(UistV1Error::UnknownBacktest)
        }
    }

    #[get("/init/{dataset_name}")]
    pub async fn init(
        app: web::Data<UistState>,
        path: web::Path<(String,)>,
    ) -> Result<web::Json<InitResponse>, UistV1Error> {
        let mut uist = app.lock().unwrap();
        let (dataset_name,) = path.into_inner();

        if let Some(backtest_id) = uist.init(dataset_name) {
            Ok(web::Json(InitResponse { backtest_id }))
        } else {
            Err(UistV1Error::UnknownDataset)
        }
    }

    #[get("/backtest/{backtest_id}/info")]
    pub async fn info(
        app: web::Data<UistState>,
        path: web::Path<(BacktestId,)>,
    ) -> Result<web::Json<InfoResponse>, UistV1Error> {
        let uist = app.lock().unwrap();
        let (backtest_id,) = path.into_inner();

        if let Some(resp) = uist.backtests.get(&backtest_id) {
            Ok(web::Json(InfoResponse {
                version: "v1".to_string(),
                dataset: resp.dataset_name.clone(),
            }))
        } else {
            Err(UistV1Error::UnknownBacktest)
        }
    }

    #[get("/backtest/{backtest_id}/now")]
    pub async fn now(
        app: web::Data<UistState>,
        path: web::Path<(BacktestId,)>,
    ) -> Result<web::Json<NowResponse>, UistV1Error> {
        let uist = app.lock().unwrap();
        let (backtest_id,) = path.into_inner();

        if let Some(backtest) = uist.backtests.get(&backtest_id) {
            let now = backtest.date;
            if let Some(dataset) = uist.datasets.get(&backtest.dataset_name) {
                let mut has_next = false;
                if dataset.has_next(backtest.pos) {
                    has_next = true;
                }
                Ok(web::Json(NowResponse { now, has_next }))
            } else {
                Err(UistV1Error::UnknownDataset)
            }
        } else {
            Err(UistV1Error::UnknownBacktest)
        }
    }
}

#[cfg(test)]
mod tests {
    use actix_web::{test, web, App};

    use rotala::exchange::uist_v1::Order;
    use rotala::input::penelope::Penelope;

    use super::server::*;
    use super::{AppState, FetchQuotesResponse, InitResponse, InsertOrderRequest, TickResponse};
    use std::sync::Mutex;

    #[actix_web::test]
    async fn test_single_trade_loop() {
        let uist = Penelope::random(100, vec!["ABC", "BCD"]);
        let dataset_name = "fake";
        let state = AppState::single(dataset_name, uist);

        let app_state = Mutex::new(state);
        let uist_state = web::Data::new(app_state);

        let app = test::init_service(
            App::new()
                .app_data(uist_state)
                .service(info)
                .service(init)
                .service(fetch_quotes)
                .service(tick)
                .service(insert_order)
                .service(delete_order),
        )
        .await;

        let req = test::TestRequest::get()
            .uri(format!("/init/{dataset_name}").as_str())
            .to_request();
        let resp: InitResponse = test::call_and_read_body_json(&app, req).await;

        let backtest_id = resp.backtest_id;

        let req1 = test::TestRequest::get()
            .uri(format!("/backtest/{backtest_id}/fetch_quotes").as_str())
            .to_request();
        let _resp1: FetchQuotesResponse = test::call_and_read_body_json(&app, req1).await;

        let req2 = test::TestRequest::get()
            .uri(format!("/backtest/{backtest_id}/tick").as_str())
            .to_request();
        let _resp2: TickResponse = test::call_and_read_body_json(&app, req2).await;

        let req3 = test::TestRequest::post()
            .set_json(InsertOrderRequest {
                order: Order::market_buy("ABC", 100.0),
            })
            .uri(format!("/backtest/{backtest_id}/insert_order").as_str())
            .to_request();
        test::call_and_read_body(&app, req3).await;

        let req4 = test::TestRequest::get()
            .uri(format!("/backtest/{backtest_id}/tick").as_str())
            .to_request();
        let _resp4: TickResponse = test::call_and_read_body_json(&app, req4).await;

        let req5 = test::TestRequest::get()
            .uri(format!("/backtest/{backtest_id}/tick").as_str())
            .to_request();
        let resp5: TickResponse = test::call_and_read_body_json(&app, req5).await;

        assert!(resp5.executed_trades.len() == 1);
        assert!(resp5.executed_trades.first().unwrap().symbol == "ABC")
    }
}
